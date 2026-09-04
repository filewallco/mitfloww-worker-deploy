import fs from 'fs';
import path from 'path';
import { config } from '../config';
import { JOB_STAGE, JOB_STATUS, REDIS_KEYS } from '../constants';
import { connection } from '../queue/connection';
import { imageQueue, largeQueue, mediumQueue, smallQueue } from '../queue/queues';
import { logger } from './logger';
import { getFreeDiskSpace } from './disk';

const ACTIVE_STATUSES = new Set<string>([
  JOB_STATUS.QUEUED,
  JOB_STATUS.RETRYING,
  JOB_STATUS.PROCESSING,
  JOB_STATUS.UPLOADING,
]);

const ACTIVE_STAGES = new Set<string>([
  JOB_STAGE.WAITING,
  JOB_STAGE.WAITING_FOR_DISK,
  JOB_STAGE.WAITING_FOR_CPU,
  JOB_STAGE.WAITING_FOR_USER_SLOT,
  JOB_STAGE.RESERVED,
  JOB_STAGE.VALIDATING,
  JOB_STAGE.DOWNLOADING,
  JOB_STAGE.PROCESSING,
  JOB_STAGE.UPLOADING,
  JOB_STAGE.DELAYED,
  JOB_STAGE.STARTING,
]);

type CleanupDecision = {
  shouldDelete: boolean;
  reason: string;
};

async function getBullState(jobId: string): Promise<string | null> {
  const job =
    (await imageQueue.getJob(jobId)) ||
    (await smallQueue.getJob(jobId)) ||
    (await mediumQueue.getJob(jobId)) ||
    (await largeQueue.getJob(jobId));

  if (!job) return null;
  return await job.getState();
}

function toNumber(value: string | undefined): number | null {
  if (!value) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

async function decideFolderCleanup(jobId: string, ageMs: number, now: number): Promise<CleanupDecision> {
  const lockExists = await connection.exists(REDIS_KEYS.LOCK(jobId));
  if (lockExists) return { shouldDelete: false, reason: 'lock_exists' };

  const reservationExists = await connection.exists(REDIS_KEYS.RESOURCE_DISK_JOB(jobId));
  if (reservationExists) return { shouldDelete: false, reason: 'disk_reservation_exists' };

  const meta = await connection.hgetall(REDIS_KEYS.JOB(jobId));
  const status = meta.status || '';
  const stage = meta.stage || '';
  const cleanupEligibleAt = toNumber(meta.tempCleanupEligibleAt);

  if (ACTIVE_STATUSES.has(status)) {
    return { shouldDelete: false, reason: `active_status:${status}` };
  }

  if (ACTIVE_STAGES.has(stage)) {
    return { shouldDelete: false, reason: `active_stage:${stage}` };
  }

  const bullState = await getBullState(jobId);
  if (bullState === 'active') {
    return { shouldDelete: false, reason: 'bull_state_active' };
  }

  if (cleanupEligibleAt && now < cleanupEligibleAt) {
    return { shouldDelete: false, reason: 'cleanup_metadata_not_due' };
  }

  if (status === JOB_STATUS.COMPLETED) {
    return {
      shouldDelete: ageMs >= config.cleanup.tempCleanupMinAgeMs,
      reason: ageMs >= config.cleanup.tempCleanupMinAgeMs ? 'completed' : 'completed_too_new',
    };
  }

  if (status === JOB_STATUS.FAILED || status === JOB_STATUS.CANCELLED) {
    return {
      shouldDelete: ageMs >= config.cleanup.failedTempRetentionMs,
      reason: ageMs >= config.cleanup.failedTempRetentionMs ? 'failed_retention_elapsed' : 'failed_too_new',
    };
  }

  if (Object.keys(meta).length === 0) {
    return {
      shouldDelete: ageMs >= config.cleanup.tempCleanupMinAgeMs,
      reason: ageMs >= config.cleanup.tempCleanupMinAgeMs ? 'orphan_old' : 'orphan_too_new',
    };
  }

  return { shouldDelete: false, reason: `status_not_deletable:${status || 'unknown'}` };
}

export async function cleanupTempDir() {
  const dir = config.tempDir;
  if (!fs.existsSync(dir)) return;

  const entries = await fs.promises.readdir(dir, { withFileTypes: true });
  const folders = await Promise.all(
    entries
      .filter((entry) => entry.isDirectory())
      .map(async (entry) => {
        const fullPath = path.join(dir, entry.name);
        const stat = await fs.promises.stat(fullPath);
        return {
          path: fullPath,
          jobId: entry.name,
          mtimeMs: stat.mtimeMs,
        };
      }),
  );

  folders.sort((a, b) => a.mtimeMs - b.mtimeMs);
  const now = Date.now();
  let freeBytes = getFreeDiskSpace();

  for (const folder of folders) {
    if (freeBytes >= config.disk.targetFreeBytes) break;

    const ageMs = now - folder.mtimeMs;
    try {
      const decision = await decideFolderCleanup(folder.jobId, ageMs, now);

      if (!decision.shouldDelete) {
        logger.info('Temp cleanup skipped', {
          jobId: folder.jobId,
          path: folder.path,
          reason: decision.reason,
          ageMs,
        });
        continue;
      }

      await fs.promises.rm(folder.path, { recursive: true, force: true });
      freeBytes = getFreeDiskSpace();
      logger.info('Temp cleanup deleted', {
        jobId: folder.jobId,
        path: folder.path,
        freeBytes,
        reason: decision.reason,
      });
    } catch (error) {
      logger.error('Temp cleanup failed for folder', {
        path: folder.path,
        jobId: folder.jobId,
        error,
      });
    }
  }
}


export async function runPeriodicMaintenance(
  retentionMs: number = Number(process.env.MAINTENANCE_RETENTION_MS || 72 * 60 * 60 * 1000)
) {
  const now = Date.now();
  logger.info('Starting periodic maintenance', { retentionMs, now });

  // 1. Clean genuinely orphaned/old temp directories older than retentionMs (3 days)
  const dir = config.tempDir;
  if (fs.existsSync(dir)) {
    try {
      const entries = await fs.promises.readdir(dir, { withFileTypes: true });
      for (const entry of entries) {
        if (!entry.isDirectory()) continue;
        const fullPath = path.join(dir, entry.name);
        const stat = await fs.promises.stat(fullPath).catch(() => null);
        if (!stat) continue;

        const ageMs = now - stat.mtimeMs;
        if (ageMs < retentionMs) continue; // Only touch folders older than retention period

        const decision = await decideFolderCleanup(entry.name, ageMs, now);
        if (decision.shouldDelete) {
          await fs.promises.rm(fullPath, { recursive: true, force: true }).catch(() => {});
          logger.info('Periodic maintenance deleted orphaned temp folder', {
            jobId: entry.name,
            path: fullPath,
            ageHours: Math.round(ageMs / 3600000),
            reason: decision.reason,
          });
        }
      }
    } catch (err) {
      logger.error('Periodic maintenance temp dir scan failed', { error: err });
    }
  }

  // 2. Clean BullMQ completed and failed job history older than retention period
  const queues = [imageQueue, smallQueue, mediumQueue, largeQueue];
  for (const queue of queues) {
    try {
      await queue.clean(retentionMs, 1000, 'completed');
      await queue.clean(retentionMs, 1000, 'failed');
    } catch (err) {
      logger.warn('Periodic maintenance BullMQ queue clean error', { queue: queue.name, error: err });
    }
  }

  // 3. Clean stale terminal Redis job hashes older than retention period
  try {
    const keys = await connection.keys('job:*');
    for (const key of keys) {
      if (key.includes(':logs') || key.includes(':preview')) continue;
      const meta = await connection.hgetall(key);
      const status = meta?.status;
      if (status === JOB_STATUS.COMPLETED || status === JOB_STATUS.FAILED || status === JOB_STATUS.CANCELLED) {
        const timestamp = Number(meta.cancelledAt || meta.completedAt || meta.failedAt || meta.updatedAt || 0);
        if (timestamp > 0 && now - timestamp >= retentionMs) {
          const jobId = key.replace(/^job:/, '');
          const lockExists = await connection.exists(REDIS_KEYS.LOCK(jobId));
          if (!lockExists) {
            await connection.del(key);
            await connection.del(REDIS_KEYS.JOB_LOGS(jobId));
            await connection.del(REDIS_KEYS.PREVIEW(jobId));
            logger.info('Periodic maintenance purged stale terminal job Redis hash', { key, status });
          }
        }
      }
    }
  } catch (err) {
    logger.warn('Periodic maintenance Redis hash scan failed', { error: err });
  }

  logger.info('Periodic maintenance completed');
}
