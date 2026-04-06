import { FileJob, JobStatus } from '../types';
import { processVideo } from '../processors/video';
import { download, upload } from '../utils/r2';
// import { acquire, release } from './admission';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { connection } from '../queue/connection';
import { enqueueFile } from '../queue/enqueue';
import { getDuration } from '../processors/video';
import { getFreeDiskSpace } from '../utils/disk';
import { config } from '../config';

const WORKER_ID = `${process.pid}-${Date.now()}`;
const LOCK_TTL = 15 * 60 * 1000;

/**
 * Determine the job category based on file size.
 * - 'small'  : <100 MB
 * - 'medium' : 100 MB – 500 MB
 * - 'large'  : >500 MB
 */
function getType(size: number): 'small' | 'medium' | 'large' {
  const MB = 1024 * 1024;
  if (size < 100 * MB) return 'small';
  if (size < 500 * MB) return 'medium';
  return 'large';
}

/**
 * Centralized helper to update job metadata in Redis.
 * Updates status, stage, and any extra info (like progress, errors, output).
 * TTL of 24 hours is set to avoid stale data.
 *
 * @param jobId - Unique identifier of the job
 * @param status - Current JobStatus
 * @param stage - Current stage of processing
 * @param extra - Optional additional metadata
 */
async function updateJobStage(
  jobId: string,
  status: JobStatus,
  stage: string,
  extra?: Record<string, any>
) {
  // Extract bullJob
  const { bullJob, ...safeExtra } = extra || {};

  // Sync BullMQ progress
  if (safeExtra?.progress !== undefined && bullJob) {
    await bullJob.updateProgress(safeExtra.progress);
  }

  const payload = {
    status,
    stage,
    ...extra,
    updatedAt: Date.now(),
  };

  /**
   * Store metadata
   */
  await connection.hset(`job:${jobId}`, payload);

  /**
   * Append log entry (NEW)
   * This allows UI timeline view
   */
  await connection.rpush(
    `job:${jobId}:logs`,
    JSON.stringify({
      time: Date.now(),
      stage,
      status,
      ...extra,
    })
  );

  /**
   * Keep logs for 24h
   */
  await connection.expire(`job:${jobId}`, 60 * 60 * 24);
  await connection.expire(`job:${jobId}:logs`, 60 * 60 * 24);
}

/**
 * Main job handler.
 * Handles the lifecycle of a file job: downloading, processing, uploading,
 * retrying on failure, and optionally moving to DLQ.
 *
 * @param job - FileJob object with metadata and file info
 * @param bullJob - Optional BullMQ job reference (for progress & retries)
 */
export async function handleJob(job: FileJob, bullJob?: any) {

  /**
   * IDEMPOTENCY LOCK (CRITICAL)
   * Prevent duplicate execution
   */
  const lockKey = `lock:${job.fileId}`;

  const acquiredLock = await connection.set(
    lockKey,
    WORKER_ID,
    'PX',
    LOCK_TTL,
    'NX'
  );

  if (acquiredLock !== 'OK') {
    console.log(`Skipping duplicate execution: ${job.fileId}`);
    return;
  }
  
  /**
   * Ensure job has initial state without overwriting enqueue metadata
   */
  await connection.hset(`job:${job.fileId}`, {
    status: 'processing',
    stage: 'starting',
    startedAt: Date.now(),
  });

  const startTime = Date.now();

  // HEARTBEAT
  const heartbeat = setInterval(async () => {
    try {
      const owner = await connection.get(lockKey);

      if (owner === WORKER_ID) {
        await connection.pexpire(lockKey, LOCK_TTL);
      } else {
        console.warn(`Lost lock ownership for ${job.fileId}`);
        clearInterval(heartbeat);
      }
    } catch (err) {
      console.error('Heartbeat error:', err);
    }
  }, 60_000);
  
  const type = getType(job.size);

  // Uncomment this for admission to handle slot allocation.
  // // Step 0: Acquire a processing slot based on job type
  // const acquired = await acquire(type);
  // if (!acquired) {
  //   const retries = job.retryCount || 0;

  //   if (retries > 3) {
  //     await updateJobStage(job.fileId, 'failed', 'admission_failed', {
  //       error: 'Admission limit exceeded',
  //     });
  //     return;
  //   }

  //   await new Promise(r => setTimeout(r, 2000));

  //   await enqueueFile({
  //     ...job,
  //     retryCount: retries + 1,
  //   });

  //   return;
  // }

  // Temporary directories and paths
  const tempDir = path.join(os.tmpdir(), job.fileId);
  const inputPath = path.join(tempDir, 'input');
  const outputPath = path.join(tempDir, 'output.mp4');

  // Ensure directories exist
  await fs.promises.mkdir(path.dirname(outputPath), { recursive: true });

  let jobStatus: JobStatus = 'processing';

  try {
    /** Stage 1: Downloading the input file */
    await updateJobStage(job.fileId, jobStatus, 'downloading', {
      startedAt: startTime,
      bullJob,
    });

    const free = getFreeDiskSpace();

    if (free < config.disk.minFreeBytes) {
      throw new Error(`Insufficient disk space: ${Math.round(free / 1e9)} GB left`);
    }

    const REQUIRED = job.size * 2; // input + output + buffer

    if (free < REQUIRED) {
      throw new Error('Not enough disk for this job');
    }

    await download(job.inputUrl, inputPath);

    /** Stage 2: Processing (only for video files) */
    if (job.fileType === 'video') {
      await updateJobStage(job.fileId, 'processing', 'processing');
    
    let duration = 0;
    try {
      duration = getDuration(inputPath);
    } catch {
      duration = 5 * 60 * 1000; // fallback
    }

    const timeoutMs = Math.max(duration * 2.5, 15 * 60 * 1000);

    // normalize FFmpeg progress → percentage
    await processVideo(inputPath, outputPath, async (timeMs: number) => {
      /**
       * WARNING:
       * This is an approximation.
       * Proper fix requires ffprobe duration (advanced).
       */

      const percent = Math.min((timeMs / duration) * 100, 100);

      await updateJobStage(job.fileId, 'processing', 'processing', {
        progress: percent,
        bullJob,
      });

      bullJob?.updateProgress(percent);
    });
    }

    /** Stage 3: Uploading the processed file */
    await updateJobStage(job.fileId, 'processing', 'uploading');
    const result = await upload(outputPath, job.outputKey);

    /** Job successfully completed */
    jobStatus = 'completed';
    await updateJobStage(job.fileId, jobStatus, 'done', {
      completedAt: Date.now(),
      duration: Date.now() - startTime,
      output: result,
      success: true,
      bullJob,
    });

  } catch (err: any) {
    // Determine if the job can still be retried
    const isRetrying = bullJob && bullJob.attemptsMade < (bullJob.opts.attempts || 1);
    jobStatus = isRetrying ? 'retrying' : 'failed';
    const MAX_TOTAL_RETRIES = 5;

    /** Update failed state in Redis */
    await updateJobStage(job.fileId, jobStatus, 'failed', {
      error: err.message,
      failedAt: Date.now(),
      attemptsMade: bullJob?.attemptsMade || 0,
      maxAttempts: bullJob?.opts.attempts || 1,
      success: false,
      bullJob,
    });

    /** Requeue logic or Dead Letter Queue (DLQ) */
    if (!isRetrying) {
      const shouldRequeue = job.size < 500 * 1024 * 1024;

      const totalRetries = job.retryCount || 0;

      /**
       * Detect poison jobs (same failure repeating)
       */
      const lastError = err.message || 'unknown';

      const prevError = await connection.hget(`job:${job.fileId}`, 'lastError');

      const isSameError = prevError === lastError;

      /**
       * If same error repeats, stop retrying early
       */
      if (isSameError && totalRetries >= 2) {
        console.log(`Poison job detected: ${job.fileId}`);

        await connection.rpush(
          'dead-letter-queue',
          JSON.stringify({ ...job, error: lastError })
        );

        return;
      }

      /**
       * Store last error for comparison
       */
      await connection.hset(`job:${job.fileId}`, 'lastError', lastError);

      if (shouldRequeue && totalRetries < MAX_TOTAL_RETRIES) {
        console.log(`Requeueing job ${job.fileId} (attempt ${totalRetries + 1})`);

        await enqueueFile({
          ...job,
          retryCount: totalRetries + 1,
        });
      } else {
        console.log(`Moving job ${job.fileId} to DLQ`);
        await connection.rpush('dead-letter-queue', JSON.stringify(job));
      }
    }

    // Bubble up the error for BullMQ to handle retry/backoff
    throw err;

  } finally {
    // await release(type); // admission release code

    /**
     * Cleanup strategy:
     * - Always clean on success
     * - Keep failed temp for retry BUT limit retention
     */
    if (jobStatus === 'completed') {
      await fs.promises.rm(tempDir, { recursive: true, force: true });
    } else {
      /**
       * Schedule delayed cleanup for failed jobs to allow for retries and debugging.
       * Prevent disk explosion
       */
      setTimeout(async () => {
        try {
          await fs.promises.rm(tempDir, { recursive: true, force: true });
        } catch { }
      }, 1000 * 60 * 30); // 30 minutes retention
    }

    // Release lock
    try {
      const owner = await connection.get(lockKey);
      if (owner === WORKER_ID) {
        await connection.del(lockKey);
      }
    } catch (err) {
      console.error('Lock release error:', err);
    }
    clearInterval(heartbeat);
  }
}