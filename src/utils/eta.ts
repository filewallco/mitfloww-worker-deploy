import { config } from '../config';
import { FILE_TYPE, MB, REDIS_KEYS } from '../constants';
import { connection } from '../queue/connection';

type SizeBucket = 'small' | 'medium' | 'large';

export function getSizeBucket(sizeBytes: number): SizeBucket {
  if (sizeBytes < 100 * MB) return 'small';
  if (sizeBytes < 2 * 1024 * MB) return 'medium';
  return 'large';
}

export function defaultProcessingEstimateMs(fileType: string, sizeBytes: number): number {
  const sizeBucket = getSizeBucket(sizeBytes);

  if (fileType === FILE_TYPE.IMAGE && sizeBucket === 'small') return 15_000;
  if (fileType === FILE_TYPE.PDF && sizeBucket === 'small') return 30_000;
  if (fileType === FILE_TYPE.VIDEO && sizeBucket === 'medium') return 15 * 60_000;
  if (fileType === FILE_TYPE.VIDEO && sizeBucket === 'large') {
    const gb = Math.max(1, Math.ceil(sizeBytes / (1024 * 1024 * 1024)));
    return gb * config.eta.videoLargeEstimateMsPerGb;
  }

  if (fileType === FILE_TYPE.IMAGE) return 25_000;
  if (fileType === FILE_TYPE.PDF) return 45_000;
  if (fileType === FILE_TYPE.VIDEO) return 20 * 60_000;
  return 45_000;
}

export async function readProcessingMetricMs(fileType: string, sizeBytes: number): Promise<number | null> {
  const sizeBucket = getSizeBucket(sizeBytes);
  const key = REDIS_KEYS.METRIC_DURATION(fileType, sizeBucket);
  const raw = await connection.hget(key, 'avgMs');
  if (!raw) return null;
  const value = Number(raw);
  return Number.isFinite(value) && value > 0 ? value : null;
}

export async function estimateProcessingMs(fileType: string, sizeBytes: number): Promise<number> {
  return (
    (await readProcessingMetricMs(fileType, sizeBytes)) ??
    defaultProcessingEstimateMs(fileType, sizeBytes)
  );
}

export async function recordProcessingDuration(
  fileType: string,
  sizeBytes: number,
  durationMs: number,
): Promise<void> {
  if (!Number.isFinite(durationMs) || durationMs <= 0) return;

  const sizeBucket = getSizeBucket(sizeBytes);
  const key = REDIS_KEYS.METRIC_DURATION(fileType, sizeBucket);
  const existing = await connection.hgetall(key);
  const prevAvg = existing.avgMs ? Number(existing.avgMs) : null;
  const prevCount = existing.count ? Number(existing.count) : 0;

  const alpha = 0.2;
  const avgMs =
    prevAvg && Number.isFinite(prevAvg)
      ? Math.round(prevAvg * (1 - alpha) + durationMs * alpha)
      : Math.round(durationMs);

  await connection.hset(key, {
    avgMs,
    count: prevCount + 1,
    updatedAt: Date.now(),
  });
}
