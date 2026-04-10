import http from 'http';
import { getSystemSnapshot } from './admin';
import { getJobDetail } from './jobDetail';
import { getDLQ } from './dlq';
import { enqueueFile } from '../queue/enqueue';
import { connection } from '../queue/connection';
import fs from 'fs';
import path from 'path';
import { FILE_TYPE, JOB_STATUS, REDIS_KEYS } from '../constants';
import { imageQueue, largeQueue, mediumQueue, smallQueue } from '../queue/queues';
import { config } from '../config';

const STATIC_ROOT = path.resolve(process.cwd(), 'outputs');
const STATIC_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Range',
  'Cache-Control': 'no-cache',
  'Accept-Ranges': 'bytes',
};

function getContentType(filePath: string): string {
  const ext = path.extname(filePath).toLowerCase();

  if (ext === '.m3u8') return 'application/vnd.apple.mpegurl';
  if (ext === '.ts') return 'video/mp2t';
  if (ext === '.mp4') return 'video/mp4';
  if (ext === '.mov') return 'video/quicktime';
  if (ext === '.webm') return 'video/webm';
  if (ext === '.png') return 'image/png';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  if (ext === '.avif') return 'image/avif';
  if (ext === '.heic') return 'image/heic';
  if (ext === '.heif') return 'image/heif';
  if (ext === '.tif' || ext === '.tiff') return 'image/tiff';
  if (ext === '.svg') return 'image/svg+xml';

  return 'application/octet-stream';
}

function normalizeStoredAssetKey(key: string): string {
  return key
    .replace(/\\/g, '/')
    .replace(/^\.?\/*outputs\//, '')
    .replace(/^\/+/, '');
}

function buildPublicAssetUrl(key: string): string {
  const normalizedKey = normalizeStoredAssetKey(key);

  return config.mode === 'local'
    ? `http://localhost:4000/static/${encodeURI(normalizedKey)}`
    : `https://your-r2-domain/${encodeURI(normalizedKey)}`;
}

function normalizePreviewKeys(keys: string[]) {
  const normalizedKeys = keys
    .map((key) => normalizeStoredAssetKey(key))
    .filter(Boolean);

  if (normalizedKeys.length === 0) return null;

  const manifestKey = normalizedKeys.find((key) => key.endsWith('.m3u8'));

  if (manifestKey) {
    const segmentKeys = normalizedKeys.filter((key) => key !== manifestKey);

    return {
      kind: 'hls',
      manifestKey,
      manifestUrl: buildPublicAssetUrl(manifestKey),
      keys: segmentKeys,
      urls: segmentKeys.map((key) => buildPublicAssetUrl(key)),
    };
  }

  if (normalizedKeys.length === 1) {
    const [key] = normalizedKeys;
    return {
      kind: 'progressive',
      key,
      url: buildPublicAssetUrl(key),
    };
  }

  return {
    kind: 'segments',
    keys: normalizedKeys,
    urls: normalizedKeys.map((key) => buildPublicAssetUrl(key)),
  };
}

function normalizePreviewPayload(value: unknown): any | null {
  if (!value) return null;

  if (Array.isArray(value)) {
    return normalizePreviewKeys(value.filter((item): item is string => typeof item === 'string'));
  }

  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      const normalized = normalizePreviewPayload(parsed);
      if (normalized) return normalized;
    } catch {
      // Legacy plain-string preview key
    }

    return normalizePreviewKeys([value]);
  }

  if (typeof value !== 'object') {
    return null;
  }

  const preview = value as Record<string, unknown>;

  if (typeof preview.key === 'string') {
    const normalizedKey = normalizeStoredAssetKey(preview.key);

    return {
      kind: preview.kind === 'hls' ? 'hls' : 'progressive',
      key: normalizedKey,
      url: buildPublicAssetUrl(normalizedKey),
      fallback: Boolean(preview.fallback),
      source: preview.source ?? 'preview',
    };
  }

  if (typeof preview.manifestKey === 'string') {
    const manifestKey = normalizeStoredAssetKey(preview.manifestKey);
    const segmentKeys = Array.isArray(preview.keys)
      ? preview.keys
          .filter((item): item is string => typeof item === 'string')
          .map((key) => normalizeStoredAssetKey(key))
      : [];

    return {
      kind: 'hls',
      manifestKey,
      manifestUrl: buildPublicAssetUrl(manifestKey),
      keys: segmentKeys,
      urls: segmentKeys.map((key) => buildPublicAssetUrl(key)),
      fallback: Boolean(preview.fallback),
      source: preview.source ?? 'preview',
    };
  }

  if (Array.isArray(preview.keys)) {
    return normalizePreviewKeys(
      preview.keys.filter((item): item is string => typeof item === 'string')
    );
  }

  return null;
}

async function resolvePreviewPayload(jobId: string) {
  const previewKey = REDIS_KEYS.PREVIEW(jobId);
  const previewType = await connection.type(previewKey);

  if (previewType === 'string') {
    const raw = await connection.get(previewKey);
    return raw ? normalizePreviewPayload(raw) : null;
  }

  if (previewType === 'list') {
    const items = await connection.lrange(previewKey, 0, -1);
    return normalizePreviewPayload(items);
  }

  if (previewType === 'hash') {
    const payload = await connection.hgetall(previewKey);
    return Object.keys(payload).length > 0
      ? normalizePreviewPayload(payload)
      : null;
  }

  return null;
}

export function startAdminServer() {
  const server = http.createServer(async (req, res) => {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Range',
      });
      return res.end();
    }

    /**
     * Static file serving for locally processed videos.
     *
     * Required for preview playback in local mode.
     * In production, this should be replaced by CDN / object storage URLs.
     */
    if (req.url?.startsWith('/static/')) {
      const requestPath = req.url.split('?')[0] || '';
      const relativePath = decodeURIComponent(requestPath.replace('/static/', ''));
      const filePath = path.resolve(STATIC_ROOT, relativePath);

      if (!filePath.startsWith(`${STATIC_ROOT}${path.sep}`) && filePath !== STATIC_ROOT) {
        res.writeHead(403, { 'Content-Type': 'application/json', ...STATIC_HEADERS });
        res.end(JSON.stringify({ error: 'Forbidden' }));
        return;
      }

      let stat: fs.Stats;

      try {
        stat = await fs.promises.stat(filePath);
      } catch {
        res.writeHead(404, { 'Content-Type': 'application/json', ...STATIC_HEADERS });
        res.end(JSON.stringify({ error: 'File not found' }));
        return;
      }

      if (!stat.isFile()) {
        res.writeHead(404, { 'Content-Type': 'application/json', ...STATIC_HEADERS });
        res.end(JSON.stringify({ error: 'File not found' }));
        return;
      }

      const contentType = getContentType(filePath);
      const range = req.headers.range;

      if (range) {
        const parts = range.replace(/bytes=/, '').split('-');
        const start = Number(parts[0]);
        const end = parts[1] ? Number(parts[1]) : stat.size - 1;

        if (!Number.isFinite(start) || !Number.isFinite(end) || start > end || start < 0 || end >= stat.size) {
          res.writeHead(416, {
            'Content-Range': `bytes */${stat.size}`,
            'Content-Type': 'application/json',
            ...STATIC_HEADERS,
          });
          res.end(JSON.stringify({ error: 'Invalid range' }));
          return;
        }

        res.writeHead(206, {
          'Content-Range': `bytes ${start}-${end}/${stat.size}`,
          'Content-Length': end - start + 1,
          'Content-Type': contentType,
          ...STATIC_HEADERS,
        });

        fs.createReadStream(filePath, { start, end }).pipe(res);
      } else {
        res.writeHead(200, {
          'Content-Length': stat.size,
          'Content-Type': contentType,
          ...STATIC_HEADERS,
        });

        fs.createReadStream(filePath).pipe(res);
      }
      return;
    }

    /**
     * Main dashboard snapshot
     */
    if (req.url === '/admin') {
      const data = await getSystemSnapshot();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
      return;
    }

    /**
     * Job detail
     */
    if (req.url?.startsWith('/admin/job/')) {
      const id = req.url.split('/').pop();

      if (!id) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid job id' }));
        return;
      }

      const data = await getJobDetail(id!);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
      return;
    }

    /**
     * Preview endpoint.
     *
     * Returns ordered list of processed video parts for progressive playback.
     * Data source: Redis list `preview:{jobId}`
     *
     * Local mode:
     *   returns URLs pointing to /static/*
     *
     * Server mode:
     *   returns URLs pointing to object storage (R2)
     */
    if (req.url?.startsWith('/preview/')) {
      const id = req.url.split('/').pop();

      if (!id) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid job id' }));
        return;
      }

      const preview = await resolvePreviewPayload(id);

      if (preview) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(preview));
        return;
      }

      const meta = await connection.hgetall(REDIS_KEYS.JOB(id));

      if (meta.status === JOB_STATUS.COMPLETED && meta.outputKey) {
        const fallbackKey = normalizeStoredAssetKey(meta.outputKey);

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          kind: 'progressive',
          key: fallbackKey,
          url: buildPublicAssetUrl(fallbackKey),
          fallback: true,
          source: 'final',
        }));
        return;
      }

      res.writeHead(404);
      res.end(JSON.stringify({ error: 'Preview not ready' }));
      return;
    }

    /**
     * DLQ
     */
    if (req.url === '/admin/dlq') {
      const data = await getDLQ();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
      return;
    }

    /**
     * Retry endpoint (NEW)
     */
    if (req.url?.startsWith('/admin/retry/')) {
      const id = req.url.split('/').pop();

      if (!id) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid job id' }));
        return;
      }

      const meta = await connection.hgetall(`job:${id}`);

      /**
       * Validate and narrow Redis string values into proper types
       */
      const fileType =
        meta.fileType === FILE_TYPE.VIDEO ||
          meta.fileType === FILE_TYPE.PDF ||
          meta.fileType === FILE_TYPE.ZIP ||
          meta.fileType === FILE_TYPE.OTHER
          ? meta.fileType
          : FILE_TYPE.OTHER;

      const userTier =
        meta.userTier === 'free' ||
          meta.userTier === 'premium' ||
          meta.userTier === 'vip'
          ? meta.userTier
          : 'free';

      if (!meta.inputUrl || !meta.outputKey) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid job metadata for retry' }));
        return;
      }

      const size = Number(meta.size);

      if (!size || isNaN(size)) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid file size' }));
        return;
      }

      await connection.hset(`job:${id}`, {
        retriedAt: Date.now(),
      });

      /**
       * Retry with validated types
       */
      await enqueueFile({
        fileId: id!,
        inputUrl: meta.inputUrl,
        outputKey: meta.outputKey,
        fileType,
        size: size,
        userTier,
      });

      res.writeHead(200);
      res.end(JSON.stringify({ success: true }));
      return;
    }

    /**
     * Public job status (USER SAFE)
     */
    if (req.url?.startsWith('/job/') && !req.url.startsWith('/job/cancel/')) {
      const id = req.url.split('/').pop();

      if (!id) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid job id' }));
        return;
      }

      const meta = await connection.hgetall(`job:${id}`);

      if (!meta || Object.keys(meta).length === 0) {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'Job not found' }));
        return;
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: meta.status,
        stage: meta.stage,
        progress: Number(meta.progress || 0),
        queueName: meta.queueName,
        createdAt: meta.createdAt,
        startedAt: meta.startedAt,
        completedAt: meta.completedAt,
        error: meta.error || null
      }));

      return;
    }

    if (req.url?.startsWith('/job/cancel/')) {
      const id = req.url.split('/').pop();

      const job =
        await imageQueue.getJob(id!) ||
        await smallQueue.getJob(id!) ||
        await mediumQueue.getJob(id!) ||
        await largeQueue.getJob(id!);

      if (job) {
        await job.remove();
      }

      await connection.hset(`job:${id}`, {
        status: 'cancelled',
        stage: 'failed',
      });

      res.writeHead(200);
      res.end(JSON.stringify({ success: true }));
      return;
    }

    res.writeHead(404);
    res.end();
  });

  server.listen(4000, () => {
    console.log('Admin API running on http://localhost:4000');
  });
}
