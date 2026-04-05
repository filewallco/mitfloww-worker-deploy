import { smallQueue, mediumQueue, largeQueue } from '../queue/queues';
import { connection } from '../queue/connection';

/**
 * Returns structured system snapshot.
 * Separates LIVE jobs and HISTORY jobs.
 */
export async function getSystemSnapshot() {
  const keys = (await connection.keys('job:*')).filter(
    k => !k.includes(':logs')
  );

  const jobs = [];

  for (const key of keys) {
    const id = key.replace('job:', '');

    const meta = await connection.hgetall(key);

    jobs.push({
      id,
      data: { inputUrl: meta.inputUrl },
      queueName: meta.queueName || 'unknown',
    });
  }
    
  /**
   * NEW: sort jobs by queuedAt (oldest first)
   * This ensures deterministic ordering in UI
   */
  const jobsWithMeta = await Promise.all(
    jobs.map(async (job) => {
      const meta = await connection.hgetall(`job:${job.id}`);
      return {
        ...job,
        queuedAt: meta.queuedAt ? Number(meta.queuedAt) : 0,
      };
    })
  );

  jobsWithMeta.sort((a, b) => a.queuedAt - b.queuedAt);
  const stats = {
    total: 0,
    waiting: 0,
    processing: 0,
    uploading: 0,
    completed: 0,
    failed: 0,
  };

  const live: any[] = [];
  const history: any[] = [];
  /**
   * Get current waiting jobs per queue
   * Needed for real queue position
   */
  const smallWaiting = await smallQueue.getJobs(['waiting']);
  const mediumWaiting = await mediumQueue.getJobs(['waiting']);
  const largeWaiting = await largeQueue.getJobs(['waiting']);

  for (const job of jobsWithMeta) {
    const meta = await connection.hgetall(`job:${job.id}`);

    const state = meta.status || 'unknown';

    /**
     * Compute queue position dynamically
     */
    let queuePosition = 0;
    

    const waitingList =
      job.queueName === 'small-files'
        ? smallWaiting
        : job.queueName === 'medium-files'
        ? mediumWaiting
        : largeWaiting;

    const index = waitingList.findIndex(
      j => j.data?.fileId === job.id
    );

    queuePosition = index >= 0 ? index + 1 : 0; // 1-based index

    const now = Date.now();

    const startedAt = meta.startedAt ? Number(meta.startedAt) : null;
    const completedAt = meta.completedAt ? Number(meta.completedAt) : null;
    const queuedAt = meta.queuedAt ? Number(meta.queuedAt) : null;
    const progress = Number(meta.progress || 0);

    const elapsed = startedAt ? now - startedAt : null;

    const speed = elapsed ? progress / elapsed : 0;
    const eta = speed > 0 ? (100 - progress) / speed : null;

    const avgProcessingTime = 30000; // start with 30s baseline

    const queueETA =
      queuePosition > 0
        ? queuePosition * avgProcessingTime
        : 0;

    const waitTime =
      startedAt && queuedAt
        ? startedAt - queuedAt
        : null;

    const formatted = {
      id: job.id,
      state,
      progress,
      fileName: job.data?.inputUrl?.split(/[\\/]/).pop(),
      size: Number(meta.size || 0),
      queuePosition,
      stage: meta.stage || 'waiting',
      userTier: meta.userTier || 'free',
      error: meta.error || null,

      startedAt,
      completedAt,
      queuedAt,

      duration: meta.duration ? Number(meta.duration) : null,

      // NEW FIELDS
      eta,
      waitTime,
      queueETA,
    };

    stats.total++;

    /**
     * Classify into LIVE vs HISTORY
     */
    if (state === 'completed') {
      stats.completed++;
      history.push(formatted);
    } else if (state === 'failed') {
      stats.failed++;
      history.push(formatted);
    } else {
      live.push(formatted);

      if (formatted.stage === 'waiting') stats.waiting++;
      if (formatted.stage === 'processing') stats.processing++;
      if (formatted.stage === 'uploading') stats.uploading++;
    }
  }

  return { stats, live, history };
}