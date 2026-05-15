import { smallQueue, mediumQueue, largeQueue, imageQueue } from './queues';
import { ATTEMPTS, FileJob } from '../types';
import { connection } from './connection';
import { logger } from '../utils/logger';
import { FILE_TYPE, JOB_STAGE, JOB_STATUS, MB, QUEUE_NAME, REDIS_KEYS } from '../constants';
import { classify, getPriority } from './priority';
import { config } from '../config';
import { assertAllowedMediaInput } from '../utils/media';

const SAFE_JOB_ID = /^[A-Za-z0-9_-]{1,128}$/;

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
  if (!SAFE_JOB_ID.test(job.fileId)) {
    throw new Error('Invalid job id');
  }

  if (!Number.isFinite(job.size) || job.size <= 0 || job.size > config.security.maxUploadBytes) {
    throw new Error('Invalid upload size');
  }

  assertAllowedMediaInput(job.fileType, null, job.inputUrl);
  assertAllowedMediaInput(job.fileType, null, job.outputKey);

  const sizeType = classify(job.size);
  /**
   * Assign a sessionId to isolate runs.
   * This prevents mixing old jobs with current execution.
   */
  const sessionId = process.env.SESSION_ID || 'dev-session';
  /**
   * Preserve retry history if exists
   * hgettall(redis) - Gets all fields of a hash (object)
   */
  const existing = await connection.hgetall(REDIS_KEYS.JOB(job.fileId));
  const retryCount = existing.retryCount
    ? Number(existing.retryCount) + 1
    : 0;

  /**
   * Store initial metadata in Redis.
   * Redis becomes the single source of truth for UI.
   */
  try {
    await connection.hset(REDIS_KEYS.JOB(job.fileId), {
      sessionId,
      status: JOB_STATUS.QUEUED,
      stage: JOB_STAGE.WAITING,
      createdAt: existing.createdAt ? Number(existing.createdAt) : Date.now(),
      queuedAt: Date.now(),
      inputUrl: job.inputUrl,
      outputKey: job.outputKey,
      fileType: job.fileType,
      size: job.size,
      userTier: job.userTier,
      progress: 0,
      retryCount,
      userId: job.userId,
      batchId: job.batchId || null,
      queueName:
        sizeType === 'small'
          ? QUEUE_NAME.SMALL
          : sizeType === 'medium'
            ? QUEUE_NAME.MEDIUM
            : QUEUE_NAME.LARGE,
    });
  } catch (e) {
    logger.error('REDIS WRITE FAILED', { error: e });
  }

  /**
   * Set a TTL of 24 hours for job metadata
   * Auto deletes key after time
   * Prevent Redis memory leak
   */ 
  await connection.expire(REDIS_KEYS.JOB(job.fileId), 60 * 60 * 24);
  
  let queue;
  
  /**
   * Select correct queue based on file size
   */
  if (job.fileType === FILE_TYPE.IMAGE || job.fileType === FILE_TYPE.PDF) {
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
