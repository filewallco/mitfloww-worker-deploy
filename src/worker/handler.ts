import { FileJob, JobStatus } from '../types';
import { processVideo } from '../processors/video';
import { download, upload } from '../utils/r2';
import { acquire, release } from './admission';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { connection } from '../queue/connection';
import { enqueueFile } from '../queue/enqueue';

/**
 * Determine the job category based on file size.
 * - 'small'  : <50 MB
 * - 'medium' : 50 MB – 500 MB
 * - 'large'  : >500 MB
 */
function getType(size: number): 'small' | 'medium' | 'large' {
  const MB = 1024 * 1024;
  if (size < 50 * MB) return 'small';
  if (size < 500 * MB) return 'medium';
  return 'large';
}

/** Sleep helper (ms) */
function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
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
  await connection.hset(`job:${jobId}`, { status, stage, ...extra });
  await connection.expire(`job:${jobId}`, 60 * 60 * 24); // 24h TTL
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
  const startTime = Date.now();
  const type = getType(job.size);

  // Step 0: Acquire a processing slot based on job type
  const acquired = await acquire(type);
  if (!acquired) throw new Error('No capacity, retrying...');

  // Temporary directories and paths
  const tempDir = path.join(os.tmpdir(), job.fileId);
  const inputPath = path.join(tempDir, 'input');
  const outputPath = path.join(tempDir, 'output.mp4');

  // Ensure directories exist
  await fs.promises.mkdir(path.dirname(outputPath), { recursive: true });

  let jobStatus: JobStatus = 'processing';

  try {
    /** Stage 1: Downloading the input file */
    await updateJobStage(job.fileId, jobStatus, 'downloading', { startedAt: startTime });
    await download(job.inputUrl, inputPath);

    /** Stage 2: Processing (only for video files) */
    if (job.fileType === 'video') {
      await updateJobStage(job.fileId, 'processing', 'processing');

      await processVideo(inputPath, outputPath, async (progress: number) => {
        // Update progress both in Redis and BullMQ
        await updateJobStage(job.fileId, 'processing', 'processing', { progress });
        bullJob?.updateProgress(progress);
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
    });

  } catch (err: any) {
    // Determine if the job can still be retried
    const isRetrying = bullJob && bullJob.attemptsMade < (bullJob.opts.attempts || 1);
    jobStatus = isRetrying ? 'retrying' : 'failed';

    /** Update failed state in Redis */
    await updateJobStage(job.fileId, jobStatus, 'failed', {
      error: err.message,
      failedAt: Date.now(),
      attemptsMade: bullJob?.attemptsMade || 0,
      maxAttempts: bullJob?.opts.attempts || 1,
      success: false,
    });

    /** Requeue logic or Dead Letter Queue (DLQ) */
    if (!isRetrying) {
      const shouldRequeue = job.size < 500 * 1024 * 1024; // Only small/medium files
      if (shouldRequeue) {
        console.log(`Requeueing job ${job.fileId}...`);
        await enqueueFile(job); // Re-add job to the queue
      } else {
        console.log(`Moving job ${job.fileId} to DLQ`);
        await connection.rpush('dead-letter-queue', JSON.stringify(job)); // Persistent DLQ
      }
    }

    // Bubble up the error for BullMQ to handle retry/backoff
    throw err;

  } finally {
    /** Release the acquired slot regardless of success/failure */
    await release(type);

    /** Clean temporary directory only if the job completed successfully */
    if (jobStatus === 'completed') {
      await fs.promises.rm(tempDir, { recursive: true, force: true });
    }
  }
}