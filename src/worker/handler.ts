import { FileJob, JobStatus } from '../types';
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

/**
 * Acquire a CPU slot (blocking with retry)
 */
async function acquireCpuSlot(): Promise<void> {
  while (true) {
    const current = Number(await connection.get(CPU_KEY) || 0);

    if (current < GLOBAL_CPU_LIMIT) {
      const ok = await connection.multi()
        .incr(CPU_KEY)
        .expire(CPU_KEY, 60) // safety TTL
        .exec();

      if (ok) return;
    }

    await new Promise(r => setTimeout(r, 100)); // backoff
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

/**
 * Reserve disk space (bytes)
 */
async function reserveDisk(bytes: number): Promise<boolean> {
  const free = getFreeDiskSpace();
  const reserved = Number(await connection.get(DISK_KEY) || 0);

  if (free - reserved < bytes) return false;

  await connection.incrby(DISK_KEY, bytes);
  return true;
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
   */
  /**
   * STEP 1: Acquire idempotency lock (LONG TTL)
   */
  const acquiredLock = await connection.set(
    lockKey,
    WORKER_ID,
    'PX',
    LOCK_TTL, // <-- FIX: no short TTL
    'NX'
  );

  if (acquiredLock !== 'OK') {
    console.log(`Skipping duplicate execution: ${job.fileId}`);
    return;
  }

  console.log(`LOCK ACQUIRED: ${job.fileId} by ${WORKER_ID}`);

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
   * Reserve disk AFTER knowing actual file size on disk
   *
   * Heuristic:
   * - input file
   * - output file (approx same size)
   * - buffer (safety)
   */
  const REQUIRED_DISK = Math.max(job.size * 2, 500 * 1024 * 1024);

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

  let outputPath = '';

  let jobStatus: JobStatus = JOB_STATUS.PROCESSING;

  try {
    /** Stage 1: Downloading the input file */
    await updateJobStage(job.fileId, jobStatus, JOB_STAGE.DOWNLOADING, {
      startedAt: startTime,
      bullJob,
    });

    const free = getFreeDiskSpace();

    if (free < config.disk.minFreeBytes) {
      throw new Error(`Insufficient disk space: ${Math.round(free / 1e9)} GB left`);
    }

    const REQUIRED = job.size * 3; // input + output + buffer

    if (free < REQUIRED) {
      throw new Error('Not enough disk for this job');
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
    // --- ONLY THE VIDEO SECTION CHANGED ---
    // Everything else remains as you wrote (already solid)

    else if (job.fileType === FILE_TYPE.VIDEO) {
      const duration = getDuration(inputPath) || 5 * 60 * 1000;

      const hlsDir = path.join(tempDir, 'hls');
      await fs.promises.mkdir(hlsDir, { recursive: true });

      const hlsOutput = path.join(hlsDir, 'index.m3u8');

      /**
       * Tracks uploaded files with timestamp (prevents memory bloat)
       */
      const uploaded = new Map<string, number>();

      /**
       * Tracks files currently being processed (prevents duplicate uploads)
       */
      const processing = new Set<string>();

      const MAX_TRACKED_FILES = 5000;

      /**
       * Upload a file if not already uploaded
       * Thread-safe via `processing` guard
       */
      async function uploadIfNeeded(file: string) {
        if (uploaded.has(file)) return;
        if (processing.has(file)) return;

        processing.add(file);

        try {
          const fullPath = path.join(hlsDir, file);
          const key = `${job.outputKey}/hls/${file}`;

          // Wait until file is fully written
          await waitForStableFile(fullPath);

          await upload(fullPath, key);

          uploaded.set(file, Date.now());

          /**
           * Cleanup old entries (prevents memory growth)
           */
          if (uploaded.size > MAX_TRACKED_FILES) {
            const now = Date.now();
            for (const [k, t] of uploaded) {
              if (now - t > 60_000) {
                uploaded.delete(k);
              }
            }
          }

        } catch (err: any) {
          if (err.code !== 'ENOENT') {
            console.error('Upload failed:', file, err);
          }
        } finally {
          processing.delete(file);
        }
      }

      /**
       * Wait until enough segments exist for playback
       * Ensures preview won't break in UI
       */
      async function waitForInitialSegments() {
        while (true) {
          try {
            const files = await fs.promises.readdir(hlsDir);
            const tsCount = files.filter(f => f.endsWith('.ts')).length;

            if (tsCount >= 3) return; // ~12 seconds buffer
          } catch { }

          await new Promise(r => setTimeout(r, 300));
        }
      }

      await new Promise((resolve, reject) => {

        /**
         * FILE WATCHER
         *
         * NOTE:
         * fs.watch is NOT reliable → only used as a trigger
         * Actual correctness is guaranteed by:
         *   - waitForStableFile
         *   - scanner fallback
         */
        const watcher = fs.watch(hlsDir, (_, filename) => {
          if (!filename) return;

          uploadIfNeeded(filename); // fire-and-forget
        });

        /**
         * FALLBACK SCANNER (CRITICAL)
         *
         * Guarantees:
         * - No missed files
         * - Handles fs.watch failures under load
         */
        const scanInterval = setInterval(async () => {
          try {
            const files = await fs.promises.readdir(hlsDir);

            for (const file of files) {
              await uploadIfNeeded(file);
            }
          } catch (err) {
            console.error('Scanner error:', err);
          }
        }, 1000);

        /**
         * FFmpeg HLS generation
         */
        const ffmpeg = spawn(config.ffmpegPath, [
          '-i', inputPath,

          '-vf', 'scale=-2:360',
          '-c:v', 'libx264',
          '-c:a', 'aac',

          '-hls_time', '4',
          '-hls_list_size', '6',
          '-hls_segment_filename', path.join(hlsDir, 'segment%03d.ts'),
          '-hls_flags', 'append_list+omit_endlist',
          '-start_number', '0',

          '-f', 'hls',
          '-progress', 'pipe:2',
          hlsOutput,
        ]);

        let buffer = '';

        /**
         * Progress parsing (line-safe)
         */
        ffmpeg.stderr.on('data', async (data) => {
          buffer += data.toString();

          const lines = buffer.split('\n');
          buffer = lines.pop() || '';

          for (const line of lines) {
            if (line.startsWith('out_time_ms=')) {
              const value = Number(line.split('=')[1]);

              if (!isNaN(value) && duration) {
                const percent = Math.min((value / duration) * 100, 100);

                await updateJobStage(
                  job.fileId,
                  JOB_STATUS.PROCESSING,
                  JOB_STAGE.PROCESSING,
                  {
                    progress: percent,
                    bullJob,
                  }
                );
              }
            }
          }
        });

        /**
         * EXPOSE PREVIEW ONLY WHEN SAFE
         *
         * Guarantees:
         * - segments exist
         * - playlist exists
         * - playlist uploaded
         */
        (async () => {
          await waitForInitialSegments();

          await waitForStableFile(hlsOutput);
          await uploadIfNeeded('index.m3u8');

          /**
           * Only expose preview AFTER:
           * - playlist exists
           * - at least 3 segments uploaded
           */
          await waitForInitialSegments();

          await waitForStableFile(hlsOutput);
          await uploadIfNeeded('index.m3u8');

          await connection.set(
            REDIS_KEYS.PREVIEW(job.fileId),
            `${job.outputKey}/hls/index.m3u8`
          );
        })();

        /**
         * FFmpeg exit handling
         */
        ffmpeg.on('close', async (code) => {
          watcher.close();
          clearInterval(scanInterval);

          // Final sync (ensures no file missed)
          const files = await fs.promises.readdir(hlsDir);
          for (const file of files) {
            await uploadIfNeeded(file);
          }

          if (code === 0) resolve(null);
          else reject(new Error(`FFmpeg failed with code ${code}`));
        });

        /**
         * Spawn failure
         */
        ffmpeg.on('error', (err) => {
          try {
            watcher.close();
            clearInterval(scanInterval);
          } catch { }

          reject(err);
        });
      });

      outputPath = hlsOutput;
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
     * Release reserved disk
     */
    try {
      await releaseDisk(job.size * 3);
    } catch { }

    /**
     * Cleanup strategy:
     * - Always clean on success
     * - Keep failed temp for retry BUT limit retention
     */
    /**
     * TEMP DEBUG MODE:
     * Do NOT delete temp after success
     */
    if (jobStatus === JOB_STATUS.COMPLETED) {
      console.log('TEMP PRESERVED (SUCCESS):', tempDir);
    }
    // Uncomment for prod to keep failed temp for debugging and retries.
    // if (jobStatus === JOB_STATUS.COMPLETED) {
    //   await fs.promises.rm(tempDir, { recursive: true, force: true });
    // } 
    else {
      /**
       * Schedule delayed cleanup for failed jobs to allow for retries and debugging.
       * Prevent disk explosion
       */

      /**
       * TEMP DEBUG MODE:
       * Do NOT delete temp after failure
       */
      console.log('TEMP PRESERVED (FAILURE):', tempDir);
      
      // Uncomment for prod to keep failed temp for debugging and retries.
      // setTimeout(async () => {
      //   try {
      //     await fs.promises.rm(tempDir, { recursive: true, force: true });
      //   } catch { }
      // }, 1000 * 60 * 30); // 30 minutes retention
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