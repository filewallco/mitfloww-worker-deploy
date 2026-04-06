import { smallQueue, mediumQueue, largeQueue } from '../queue/queues';
import { connection } from '../queue/connection';
import { enqueueFile } from '../queue/enqueue';
import { imageQueue } from '../queue/queues';

/**
 * Returns structured system snapshot.
 * Separates LIVE jobs and HISTORY jobs.
 */
export async function getSystemSnapshot() {
  const keys = (await scanKeys('job:[^:]*')).filter(
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
  const smallCount = await smallQueue.getWaitingCount();
  const mediumCount = await mediumQueue.getWaitingCount();
  const largeCount = await largeQueue.getWaitingCount();

  /**
   * Iterate with index to compute queue position
   */
  for (let i = 0; i < jobsWithMeta.length; i++) {
    const job = jobsWithMeta[i];
    const meta = await connection.hgetall(`job:${job.id}`);

    /**
     * Resolve actual job state using BullMQ as runtime truth
     * Redis is treated as UI metadata layer
     */
    let queueState = null;

    const jobInstance =
      await imageQueue.getJob(job.id) ||
      await smallQueue.getJob(job.id) ||
      await mediumQueue.getJob(job.id) ||
      await largeQueue.getJob(job.id);

    if (jobInstance) {
      queueState = await jobInstance.getState();
    }

    /**
     * Final resolved state:
     * Priority:
     * 1. BullMQ runtime state
     * 2. Redis stored state
     */
    const state = queueState || meta.status || 'unknown';

    /**
     * Compute queue position based on resolved state
     * Only applies to jobs that are still waiting in queue
     */
    let queuePosition = 0;

    if (queueState === 'waiting') {
      let queueRef =
        job.queueName === 'small-files'
          ? smallQueue
          : job.queueName === 'medium-files'
            ? mediumQueue
            : largeQueue;

      const waitingJobs = await queueRef.getWaiting();

      const index = waitingJobs.findIndex(j => j.id === job.id);

      queuePosition = index >= 0 ? index + 1 : 0;
    }

    const now = Date.now();

    const startedAt = meta.startedAt ? Number(meta.startedAt) : null;
    const completedAt = meta.completedAt ? Number(meta.completedAt) : null;
    const queuedAt = meta.queuedAt ? Number(meta.queuedAt) : null;
    const progress = Number(meta.progress || 0);

    const elapsed = startedAt ? now - startedAt : null;

    const speed = elapsed ? progress / elapsed : 0;
    const eta = speed > 0 ? (100 - progress) / speed : null;

    let queueSize = 0;
    if (job.queueName === 'small-files') queueSize = smallCount;
    else if (job.queueName === 'medium-files') queueSize = mediumCount;
    else queueSize = largeCount;

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

/**
   * SAFE SCAN KEYS
   */
async function scanKeys(pattern: string): Promise<string[]> {
  let cursor = '0';
  const keys: string[] = [];

  do {
    const [nextCursor, results] = await connection.scan(
      cursor,
      'MATCH',
      pattern,
      'COUNT',
      100
    );

    cursor = nextCursor;
    keys.push(...results);
  } while (cursor !== '0');

  return keys;
}

/**
 * Detect and recover stuck jobs
 * A job is considered stuck if:
 * - status is processing
 * - no update for > X time
 */
export async function recoverStuckJobs() {
  const keys = (await scanKeys('job:[^:]*')).filter(
    k => !k.includes(':logs')
  );

  const now = Date.now();
  const STUCK_THRESHOLD = 5 * 60 * 1000; // 5 minutes

  for (const key of keys) {
    const meta = await connection.hgetall(key);
    const jobId = key.replace('job:', '');

    const jobInstance =
      await imageQueue.getJob(jobId) ||
      await smallQueue.getJob(jobId) ||
      await mediumQueue.getJob(jobId) ||
      await largeQueue.getJob(jobId);

    let state = null;

    if (jobInstance) {
      state = await jobInstance.getState();
    }

    /**
     * Skip if actually active in BullMQ
     */
    if (state === 'active') continue;

    if (meta.status === 'processing') {
      const updatedAt = Number(meta.updatedAt || 0);

      if (now - updatedAt > STUCK_THRESHOLD) {
        console.log(`Recovering stuck job: ${jobId}`);

        await connection.hset(key, {
          status: 'retrying',
          stage: 'stuck_recovery',
        });

        const existing =
          await smallQueue.getJob(jobId) ||
          await mediumQueue.getJob(jobId) ||
          await largeQueue.getJob(jobId);

        if (existing) {
          await existing.remove();
        }

        await enqueueFile({
          fileId: jobId,
          inputUrl: meta.inputUrl,
          outputKey: meta.outputKey,
          fileType: meta.fileType as any,
          size: Number(meta.size),
          userTier: meta.userTier as any,
        });
      }
    }
  }
}