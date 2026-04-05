import http from 'http';
import { getSystemSnapshot } from './admin';
import { getJobDetail } from './jobDetail';
import { getDLQ } from './dlq';
import { enqueueFile } from '../queue/enqueue';
import { connection } from '../queue/connection';

export function startAdminServer() {
  const server = http.createServer(async (req, res) => {

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