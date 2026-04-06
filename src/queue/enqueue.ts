import { smallQueue, mediumQueue, largeQueue, imageQueue } from './queues';
import { ATTEMPTS, FileJob } from '../types';
import { connection } from './connection';

const MB = 1024 * 1024;

/**
 * Classifies file size into categories
 * @param size - Size of the file in bytes
 * @returns 'small' | 'medium' | 'large'
 */
function classify(size: number): 'small' | 'medium' | 'large' {
  if (size < 100 * MB) return 'small';
  if (size < 500 * MB) return 'medium';
  return 'large';
}

/**
 * Computes base priority for a job based on:
 * 1. User tier: VIP < Premium < Free
 * 2. File size: Small < Medium < Large
 * Lower values indicate higher priority
 *
 * @param job - File job object
 * @returns numeric base priority
 */
function basePriority(job: FileJob): number {
  if (job.fileType === 'image') return -10;
  const sizeType = classify(job.size);

  const tierWeight = {
    vip: 0,
    premium: 1,
    free: 2,
  };

  const sizeWeight = {
    small: 0,
    medium: 1,
    large: 2,
  };

  return tierWeight[job.userTier] * 10 + sizeWeight[sizeType];
}

/**
 * Computes final priority including aging to prevent starvation.
 * Uses bounded aging based on waiting time.
 *
 * IMPORTANT:
 * - Lower number = higher priority in BullMQ
 * - Aging reduces priority value over time (boosts older jobs)
 */
async function getPriority(job: FileJob): Promise<number> {
  const base = basePriority(job);

  /**
   * Use a small aging factor to boost older jobs over time.
   * This prevents starvation of lower-priority jobs.
   * Example:
   * - Every minute reduces priority slightly
   */
  const agingFactor = 0.1; // tuneable
  const createdAtRaw = await connection.hget(`job:${job.fileId}`, 'createdAt');
  const createdAt = createdAtRaw ? Number(createdAtRaw) : Date.now();

  const waitingMinutes = (Date.now() - createdAt) / 60000;

  return Math.floor(base - waitingMinutes * agingFactor);
}

/**
 * Enqueues a file processing job:
 * - Stores job metadata in Redis (status, stage, size, user tier)
 * - Sets a TTL for the job metadata
 * - Calculates queue position
 * - Adds job to BullMQ queue with priority, retry, and backoff settings
 *
 * @param job - File job to enqueue
 * @returns Promise resolving to the BullMQ Job object
 */
export async function enqueueFile(job: FileJob) {
  const sizeType = classify(job.size);

  /**
   * Assign a sessionId to isolate runs.
   * This prevents mixing old jobs with current execution.
   */
  const sessionId = process.env.SESSION_ID || 'dev-session';

  /**
   * Preserve retry history if exists
   */
  const existing = await connection.hgetall(`job:${job.fileId}`);

  const retryCount = existing.retryCount
    ? Number(existing.retryCount) + 1
    : 0;

  /**
   * Store initial metadata in Redis.
   * Redis becomes the single source of truth for UI.
   */
  try {
    await connection.hset(`job:${job.fileId}`, {
      sessionId,
      status: 'queued',
      stage: 'waiting',
      createdAt: existing.createdAt ? Number(existing.createdAt) : Date.now(),
      queuedAt: Date.now(),

      inputUrl: job.inputUrl,
      outputKey: job.outputKey,
      fileType: job.fileType,

      size: job.size,
      userTier: job.userTier,
      progress: 0,

      /**
       * NEW: track retries
       */
      retryCount,
      queueName:
        sizeType === 'small'
          ? 'small-files'
          : sizeType === 'medium'
            ? 'medium-files'
            : 'large-files',
    });
  } catch (e) {
    console.error('REDIS WRITE FAILED', e);
  }

  // Set a TTL of 24 hours for job metadata
  await connection.expire(`job:${job.fileId}`, 60 * 60 * 24);
  /**
   * Select correct queue based on file size
   */
  let queue;

  if (job.fileType === 'image') {
    queue = imageQueue;
  } else {
    if (sizeType === 'small') queue = smallQueue;
    else if (sizeType === 'medium') queue = mediumQueue;
    else queue = largeQueue;
  }

  /**
   * Add job to the appropriate queue
   */
  return queue.add(sizeType, job, {
    jobId: job.fileId,
    priority: await getPriority(job),
    attempts: ATTEMPTS[sizeType],
    backoff: {
      type: 'exponential',
      delay: 5000,
    },
    removeOnComplete: false,
  });
}