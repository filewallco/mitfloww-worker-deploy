import { FileJob, JobStatus } from '../types';
import { download, upload } from '../utils/r2';
// import { acquire, release } from './admission';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { connection } from '../queue/connection';
import { enqueueFile } from '../queue/enqueue';
import { generatePreviewClip, getDuration, processVideo } from '../processors/video';
import { getFreeDiskSpace } from '../utils/disk';
import { config } from '../config';
import { processImage } from '../processors/image';
import { spawn } from 'child_process';
import { FILE_TYPE, JOB_STAGE, JOB_STATUS, REDIS_KEYS } from '../constants';

const USER_ACTIVE_KEY = (userId: string) => `user:${userId}:active`;
const MAX_ACTIVE_PER_USER = 100; // TODO: make this configurable per user tier

function getCpuLimit(): number {
  // 1. Respect explicit override
  if (process.env.GLOBAL_CPU_LIMIT) {
    return Number(process.env.GLOBAL_CPU_LIMIT);
  }

  // 2. Try Docker CPU quota (cgroup v2)
  try {
    const data = require('fs').readFileSync('/sys/fs/cgroup/cpu.max', 'utf8');
    const [quota, period] = data.trim().split(' ');

    if (quota !== 'max') {
      return Math.max(1, Math.floor(Number(quota) / Number(period)));
    }
  } catch { }

  // 3. Fallback to host cores
  return os.cpus().length;
}

/**
 * GLOBAL CPU LIMITER
 *
 * Purpose:
 * Prevents total CPU oversubscription across all workers.
 * Even if per-queue concurrency is high, this enforces a hard global cap.
 *
 * Design:
 * - Uses Redis as a distributed semaphore
 * - Ensures correctness across multiple processes/containers
 *
 * NOTE:
 * - Keep this conservative (e.g., <= number of CPU cores)
 */
const GLOBAL_CPU_LIMIT = getCpuLimit();
const CPU_KEY = 'global:cpu';

const ACQUIRE_CPU_LUA = `
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local limit = tonumber(ARGV[1])

if current < limit then
  current = redis.call("INCR", KEYS[1])
  redis.call("EXPIRE", KEYS[1], 60)
  return 1
else
  return 0
end
`;

/**
 * Acquire a CPU slot (blocking with retry)
 */
async function acquireCpuSlot(): Promise<void> {
  while (true) {
    const ok = await connection.eval(
      ACQUIRE_CPU_LUA,
      1,
      CPU_KEY,
      GLOBAL_CPU_LIMIT
    );

    if (ok === 1) return;

    await new Promise(r => setTimeout(r, 100));
  }
}

/**
 * Release a CPU slot
 */
async function releaseCpuSlot(): Promise<void> {
  try {
    const val = await connection.decr(CPU_KEY);
    if (val <= 0) await connection.del(CPU_KEY);
  } catch { }
}

/**
 * GLOBAL DISK RESERVATION TRACKER
 *
 * Problem:
 * Multiple workers may pass disk check simultaneously → crash later.
 *
 * Solution:
 * Reserve disk space in Redis BEFORE processing starts.
 * Guarantees system-wide disk safety.
 */
const DISK_KEY = 'global:disk_reserved';

const RESERVE_DISK_LUA = `
local reserved = tonumber(redis.call("GET", KEYS[1]) or "0")
local free = tonumber(ARGV[1])
local required = tonumber(ARGV[2])

if (free - reserved) >= required then
  redis.call("INCRBY", KEYS[1], required)
  redis.call("EXPIRE", KEYS[1], 60)
  return 1
else
  return 0
end
`;

/**
 * Reserve disk space (bytes)
 */
async function reserveDisk(bytes: number): Promise<boolean> {
  const free = getFreeDiskSpace();
  const ok = await connection.eval(
    RESERVE_DISK_LUA,
    1,
    DISK_KEY,
    free,
    bytes
  );

  return ok === 1;
}

/**
 * Release reserved disk
 */
async function releaseDisk(bytes: number): Promise<void> {
  try {
    const val = await connection.decrby(DISK_KEY, bytes);
    if (val <= 0) await connection.del(DISK_KEY);
  } catch { }
}

/**
 * Ensures file is fully written before processing/uploading.
 *
 * WHY:
 * - fs.watch fires BEFORE write completion
 * - Uploading partial .ts files causes:
 *   - corrupt HLS segments
 *   - broken playback
 *
 * STRATEGY:
 * - Poll file size until stable
 */
async function waitForStableFile(filePath: string): Promise<void> {
  let lastSize = -1;

  while (true) {
    try {
      const stat = await fs.promises.stat(filePath);

      if (stat.size === lastSize) break;

      lastSize = stat.size;
      await new Promise((r) => setTimeout(r, 200));

    } catch (err: any) {
      if (err.code === 'ENOENT') {
        // File not ready yet → wait
        await new Promise((r) => setTimeout(r, 200));
        continue;
      }
      throw err;
    }
  }
}

const WORKER_ID = `${process.pid}-${Date.now()}`;
const LOCK_TTL = 60 * 60 * 1000;

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

function computeFileTTL(size: number): number {
  const MB = 1024 * 1024;

  const base = 5 * 60 * 1000; // 5 min
  const per100MB = 2 * 60 * 1000;

  return base + Math.ceil(size / (100 * MB)) * per100MB;
}

function computeVideoTTL(durationMs: number): number {
  const MULTIPLIER = 2.5; // encoding + overhead
  const BUFFER = 10 * 60 * 1000; // 10 min safety

  return Math.max(
    durationMs * MULTIPLIER + BUFFER,
    30 * 60 * 1000 // minimum 30 min
  );
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
   * Use controlled temp directory instead of OS temp
   * Prevents hidden temp files and debugging issues
   */
  const tempDir = path.join(config.tempDir, job.fileId);
  const inputPath = path.join(tempDir, 'input');
  const outputBase = path.join(tempDir, 'output');
  const lockKey = REDIS_KEYS.LOCK(job.fileId);
  /**
   * IDEMPOTENCY LOCK (CRITICAL)
   * Prevent duplicate execution
   * 
   * STEP 1: Acquire idempotency lock (LONG TTL)
   */
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

  console.log(`LOCK ACQUIRED: ${job.fileId} by ${WORKER_ID}`);

  //  #region admission control lock
  // /**
  //  * STEP 2: Admission control (CRITICAL)
  //  */
  // const type = getType(job.size);

  // const acquired = await acquire(type);

  // if (!acquired) {
  //   const retries = job.retryCount || 0;

  //   if (retries > 3) {
  //     await updateJobStage(job.fileId, JOB_STATUS.FAILED, 'admission_failed', {
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
  // # endregion
  /**
   * PER-USER LIMIT
   */
  const userKey = USER_ACTIVE_KEY(job.userTier); // TODO: (you should use userId later)
  const active = Number(await connection.get(userKey) || 0);

  if (active >= MAX_ACTIVE_PER_USER) {
    throw new Error('User concurrency limit reached');
  }

  await connection.incr(userKey);
  await connection.expire(userKey, 3600);

  /**
   * Acquire global CPU slot BEFORE heavy work begins
   */
  await acquireCpuSlot();

  /**
   * STEP 3: Prepare temp dir BEFORE download
   */
  await fs.promises.mkdir(tempDir, { recursive: true });

  /**
   * STEP 4: Download AFTER admission (FIXED)
   */
  await download(job.inputUrl, inputPath);

  /**
   * Calculate required disk:
   * - 2x file size (input + output)
   * - minimum 500MB safety buffer
   */
  const REQUIRED_DISK = Math.max(job.size * 2, 500 * 1024 * 1024);

  /**
   * Persist exact reservation value.
   * CRITICAL: This must be used during release to prevent drift.
   */
  await connection.hset(REDIS_KEYS.JOB(job.fileId), {
    reservedDisk: REQUIRED_DISK
  });

  /**
   * Attempt to reserve disk atomically
   */
  const reserved = await reserveDisk(REQUIRED_DISK);

  if (!reserved) {
    throw new Error('Global disk reservation failed (insufficient space)');
  }

  /**
   * STEP 5: Compute accurate TTL AFTER download
   */
  let ttl = computeFileTTL(job.size);

  if (job.fileType === FILE_TYPE.VIDEO) {
    /**
     * SAFELY extract duration (MKV-safe)
     */
    let duration = 5 * 60 * 1000; // fallback 5 min

    try {
      duration = getDuration(inputPath);
    } catch (err) {
      console.warn('FFPROBE FAILED, using fallback duration:', job.fileId);
    }

    ttl = computeVideoTTL(duration);
  }

  await connection.pexpire(lockKey, Math.max(ttl, LOCK_TTL));
  /**
   * Ensure job has initial state without overwriting enqueue metadata
   */
  await connection.hset(REDIS_KEYS.JOB(job.fileId), {
    status: JOB_STATUS.PROCESSING,
    stage: JOB_STAGE.STARTING,
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


  // #region Uncomment this for admission to handle slot allocation.
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
  // #endregion

  let outputPath = '';

  let jobStatus: JobStatus = JOB_STATUS.PROCESSING;

  try {
    /** Stage 1: Downloading the input file */
    await updateJobStage(job.fileId, jobStatus, JOB_STAGE.DOWNLOADING, {
      startedAt: startTime,
      bullJob,
    });

    const free = getFreeDiskSpace();
    const REQUIRED = job.size * 3; // input + output + buffer
    const reserved = Number(await connection.get(DISK_KEY) || 0);

    if (free - reserved < REQUIRED) {
      throw new Error('Not enough disk (after reservation)');
    }

    /** Stage 2: Processing */
    await updateJobStage(job.fileId, JOB_STATUS.PROCESSING, JOB_STAGE.PROCESSING);

    if (job.fileType === FILE_TYPE.IMAGE) {
      const result = await processImage(inputPath, outputBase);
      outputPath = result.outputPath;

      job.outputKey = job.outputKey.replace(/\.\w+$/, result.ext);
    }

    if (job.fileType === FILE_TYPE.IMAGE) {
      await updateJobStage(job.fileId, JOB_STATUS.PROCESSING, JOB_STAGE.PROCESSING, {
        progress: 100,
        bullJob,
      });
    }
    // Everything else remains as you wrote (already solid)

    else if (job.fileType === FILE_TYPE.VIDEO) {
      const previewPath = path.join(tempDir, 'preview.mp4');

      /**
       * STEP 1: Generate preview (cheap)
       */
      await generatePreviewClip(inputPath, previewPath, 8);

      /**
       * STEP 2: Upload preview early
       */
      const previewKey = `${job.outputKey}/preview.mp4`;

      await upload(previewPath, previewKey);

      /**
       * Store preview reference
       */
      await connection.set(
        REDIS_KEYS.PREVIEW(job.fileId),
        previewKey
      );

      /**
       * STEP 3: Full processing (single output)
       */
      const finalOutput = `${outputBase}.mp4`;

      await processVideo(inputPath, finalOutput, undefined, async (progress) => {
        await updateJobStage(
          job.fileId,
          JOB_STATUS.PROCESSING,
          JOB_STAGE.PROCESSING,
          {
            progress,
            bullJob,
          }
        );
      });

      outputPath = finalOutput;
    }

    /** Stage 3: Uploading the processed file */
    await updateJobStage(job.fileId, JOB_STATUS.PROCESSING, JOB_STAGE.UPLOADING);
    let result = "";
    // For video, HLS already uploaded → skip final upload
    if (job.fileType !== FILE_TYPE.VIDEO) {
      result = await upload(outputPath, job.outputKey);
    }

    /** Job successfully completed */
    jobStatus = JOB_STATUS.COMPLETED;
    await updateJobStage(job.fileId, jobStatus, JOB_STAGE.DONE, {
      completedAt: Date.now(),
      duration: Date.now() - startTime,
      output: result,
      success: true,
      bullJob,
    });

  } catch (err: any) {
    console.error('JOB FAILED:', job.fileId, err);
    // Determine if the job can still be retried
    const isRetrying = bullJob && bullJob.attemptsMade < (bullJob.opts.attempts || 1);
    jobStatus = isRetrying ? JOB_STATUS.RETRYING : JOB_STATUS.FAILED;
    const MAX_TOTAL_RETRIES = 5;

    /** Update failed state in Redis */
    await updateJobStage(job.fileId, jobStatus, JOB_STAGE.FAILED, {
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

      const prevError = await connection.hget(REDIS_KEYS.JOB(job.fileId), 'lastError');

      const normalize = (e: string) => e.split('\n')[0]; // first line only
      const isSameError = normalize(prevError || '') === normalize(lastError);

      /**
       * If same error repeats, stop retrying early
       */
      if (isSameError && totalRetries >= 2) {
        console.log(`Poison job detected: ${job.fileId}`);

        await connection.rpush(
          REDIS_KEYS.DLQ,
          JSON.stringify({ ...job, error: lastError })
        );

        return;
      }

      /**
       * Store last error for comparison
       */
      await connection.hset(REDIS_KEYS.JOB(job.fileId), 'lastError', lastError);

      if (shouldRequeue && totalRetries < MAX_TOTAL_RETRIES) {
        console.log(`Requeueing job ${job.fileId} (attempt ${totalRetries + 1})`);

        await enqueueFile({
          ...job,
          retryCount: totalRetries + 1,
        });
      } else {
        console.log(`Moving job ${job.fileId} to DLQ`);
        await connection.rpush(REDIS_KEYS.DLQ, JSON.stringify(job));
      }
    }

    // Bubble up the error for BullMQ to handle retry/backoff
    throw err;

  } finally {
    // await release(type); // admission release code
    /**
     * Release global CPU slot
     */
    await releaseCpuSlot();

    /**
     * Release user slot
     */
    try {
      const val = await connection.decr(userKey);
      if (val <= 0) await connection.del(userKey);
    } catch { }

    /**
     * Release EXACT reserved disk.
     * Prevents drift between reserve/release values.
     */
    try {
      const reservedDisk = Number(
        await connection.hget(REDIS_KEYS.JOB(job.fileId), 'reservedDisk') || 0
      );

      if (reservedDisk > 0) {
        await releaseDisk(reservedDisk);
      }
    } catch {
      // swallow — cleanup must never crash worker
    }

    /**
     * Cleanup strategy:
     * - Always clean on success
     * - Keep failed temp for retry BUT limit retention
     */
    const previewExists = await connection.get(REDIS_KEYS.PREVIEW(job.fileId));
    if (jobStatus === JOB_STATUS.COMPLETED && previewExists) {
      await fs.promises.rm(tempDir, { recursive: true, force: true });
    }
    else {
      /**
       * Schedule delayed cleanup for failed jobs to allow for retries and debugging.
       * Prevent disk explosion
       */
      setTimeout(async () => {
        try {
          const previewExists = await connection.get(REDIS_KEYS.PREVIEW(job.fileId));
          if (jobStatus === JOB_STATUS.COMPLETED && previewExists) {
            await fs.promises.rm(tempDir, { recursive: true, force: true });
          }
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