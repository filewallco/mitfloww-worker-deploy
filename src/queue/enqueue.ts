import { smallQueue, mediumQueue, largeQueue, imageQueue } from './queues';
import { FileJob } from '../types';
import { connection } from './connection';
import { logger } from '../utils/logger';
import { config } from '../config';
import { assertAllowedMediaInput } from '../utils/media';
import { FILE_TYPE, JOB_STAGE, JOB_STATUS, QUEUE_NAME, REDIS_KEYS } from "../constants";
import { classify, getPriority } from "./priority";

const SAFE_JOB_ID = /^[A-Za-z0-9_-]{1,128}$/;
const JOB_META_TTL_SECONDS = 60 * 60 * 24;
const JOB_META_TTL_MS = JOB_META_TTL_SECONDS * 1000;

async function reserveQueuedFileVersion(fileVersionId: string, jobId: string): Promise<void> {
  const queuedKey = REDIS_KEYS.QUEUED_FILE_VERSION(fileVersionId);

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const reserved = await connection.set(queuedKey, jobId, 'PX', JOB_META_TTL_MS, 'NX');
    if (reserved === 'OK') {
      return;
    }

    const owner = await connection.get(queuedKey);
    if (owner === jobId) {
      await connection.pexpire(queuedKey, JOB_META_TTL_MS);
      return;
    }

    if (owner) {
      throw new Error('duplicate_queued_file_version');
    }
  }

  throw new Error('queued_file_version_reservation_failed');
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
  if (!SAFE_JOB_ID.test(job.fileId)) {
    throw new Error('Invalid job id');
  }

  if (!Number.isFinite(job.size) || job.size <= 0 || job.size > config.security.maxUploadBytes) {
    throw new Error('Invalid upload size');
  }

  const declaredInputValue =
    job.sourceKey ??
    job.inputUrl ??
    job.originalName ??
    job.outputKey;

  assertAllowedMediaInput(job.fileType, job.mimeType ?? null, declaredInputValue);
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
  const retryCount = existing.retryCount ? Number(existing.retryCount) + 1 : 0;

  const inputRef =
    job.inputUrl ??
    (job.sourceBucket && job.sourceKey
      ? `r2://${job.sourceBucket}/${job.sourceKey}`
      : declaredInputValue);

  const queueName =
    job.fileType === FILE_TYPE.IMAGE || job.fileType === FILE_TYPE.PDF
      ? QUEUE_NAME.IMAGE
      : sizeType === "small"
        ? QUEUE_NAME.SMALL
        : sizeType === "medium"
          ? QUEUE_NAME.MEDIUM
          : QUEUE_NAME.LARGE;

  if (job.fileVersionId) {
    const activeOwner = await connection.get(REDIS_KEYS.ACTIVE_FILE_VERSION(job.fileVersionId));
    if (activeOwner && activeOwner !== job.fileId) {
      throw new Error('duplicate_active_file_version');
    }

    await reserveQueuedFileVersion(job.fileVersionId, job.fileId);
  }

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
      inputUrl: inputRef,
      sourceBucket: job.sourceBucket ?? "",
      sourceKey: job.sourceKey ?? "",
      outputBucket: job.outputBucket ?? "",
      outputKey: job.outputKey,
      logKey: job.logKey ?? "",
      fileVersionId: job.fileVersionId ?? "",
      fileName: job.fileName ?? "",
      originalName: job.originalName ?? "",
      mimeType: job.mimeType ?? "",
      extension: job.extension ?? "",
      fileType: job.fileType,
      size: job.size,
      userTier: job.userTier,
      progress: 0,
      retryCount,
      maxAttempts: config.processing.maxAttempts,
      attemptsMade: 0,
      resourceWaitCount:
        existing.resourceWaitCount && Number(existing.resourceWaitCount) > 0
          ? Number(existing.resourceWaitCount)
          : "",

      firstResourceWaitAt:
        existing.firstResourceWaitAt && Number(existing.firstResourceWaitAt) > 0
          ? Number(existing.firstResourceWaitAt)
          : "",
      userId: job.userId,
      batchId: job.batchId || "",
      callbackUrl: job.callbackUrl ?? "",
      callbackToken: job.callbackToken ?? "",
      queueName,
      isLargeFile: job.isLargeFile ? "1" : "0",
      isPreviewGeneration: job.isPreviewGeneration ? "1" : "0",
    });
  } catch (e) {
    logger.error("REDIS WRITE FAILED", { error: e });
  }

  /**
   * Set a TTL of 24 hours for job metadata
   * Auto deletes key after time
   * Prevent Redis memory leak
   */
  await connection.expire(REDIS_KEYS.JOB(job.fileId), JOB_META_TTL_SECONDS);

  const queue =
    queueName === QUEUE_NAME.IMAGE
      ? imageQueue
      : queueName === QUEUE_NAME.SMALL
        ? smallQueue
        : queueName === QUEUE_NAME.MEDIUM
          ? mediumQueue
          : largeQueue;

  /**
   * Add job to the appropriate queue
   */
  try {
    return queue.add(sizeType, job, {
      jobId: job.fileId,
      priority: await getPriority(job),
      attempts: config.processing.maxAttempts,
      backoff: {
        type: 'exponential',
        delay: 5000,
      },
      removeOnComplete: false,
    });
  } catch (error) {
    if (job.fileVersionId) {
      const queuedKey = REDIS_KEYS.QUEUED_FILE_VERSION(job.fileVersionId);
      const owner = await connection.get(queuedKey);
      if (owner === job.fileId) {
        await connection.del(queuedKey);
      }
    }
    throw error;
  }
}
