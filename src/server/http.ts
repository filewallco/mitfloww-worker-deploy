import http from 'http';
import { getSystemSnapshot } from './admin';
import { getJobDetail } from './jobDetail';
import { getDLQ } from './dlq';
import { enqueueFile } from '../queue/enqueue';
import { connection } from '../queue/connection';
import fs from 'fs';
import path from 'path';

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
      const relativePath = req.url.replace('/static/', '');

      const filePath = path.join(process.cwd(), 'outputs', relativePath);

      if (!fs.existsSync(filePath)) {
        res.writeHead(404);
        res.end();
        return;
      }

      const stream = fs.createReadStream(filePath);
      const ext = path.extname(filePath);

      const contentType =
        ext === '.m3u8'
          ? 'application/vnd.apple.mpegurl'
          : ext === '.ts'
          ? 'video/mp2t'
          : 'video/mp4';
      res.writeHead(200, {
        'Content-Type': contentType,

        // REQUIRED FOR HLS SEGMENT FETCHING
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Range',

        // REQUIRED FOR VIDEO STREAMING
        'Accept-Ranges': 'bytes',
      });

      stream.pipe(res);
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

      const key = await connection.get(`preview:${id}`);

      if (!key) {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'Preview not ready' }));
        return;
      }

      const url =
        process.env.MODE === 'local'
          ? `http://localhost:4000/static/${key}`
          : `https://your-r2-domain/${key}`;

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(url));
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
        meta.fileType === 'video' ||
          meta.fileType === 'pdf' ||
          meta.fileType === 'zip' ||
          meta.fileType === 'other'
          ? meta.fileType
          : 'other';

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

    res.writeHead(404);
    res.end();
  });

  server.listen(4000, () => {
    console.log('Admin API running on http://localhost:4000');
  });
}