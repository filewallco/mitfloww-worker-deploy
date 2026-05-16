import { FileJob, JobStatus } from '../types';
import { downloadFromR2, uploadJsonToR2, uploadToR2, upload, download } from "../utils/r2";
// import { acquire, release } from './admission';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { connection } from '../queue/connection';
import { enqueueFile } from '../queue/enqueue';
import { assertAllowedVideoProbe, inspectVideoInput, processVideo } from '../processors/video';
import { getFreeDiskSpace } from '../utils/disk';
import { config } from '../config';
import { processImage } from '../processors/image';
import { processPdf } from '../processors/pdf';
import { spawn } from 'child_process';
import { FILE_TYPE, JOB_STAGE, JOB_STATUS, REDIS_KEYS } from '../constants';
import { logger } from '../utils/logger';
import { fileTypeFromFile } from 'file-type';
import { assertAllowedMediaInput, assertDetectedMediaMatchesDeclaration, isLikelyMatroska, normalizeExtension } from '../utils/media';
import { assertBasicFileHeader } from '../utils/fileSignature';
import { toPublicErrorMessage } from '../security/errors';

const USER_ACTIVE_KEY = (userId: string) => `user:${userId}:active`;
const SAFE_JOB_ID = /^[A-Za-z0-9_-]{1,128}$/;
const LOCAL_PROTOCOL_ARGS = ['-safe', '1', '-protocol_whitelist', 'file,pipe'];

/** Maximum concurrent active jobs per user */
const ACQUIRE_USER_SLOT_LUA = `
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local limit = tonumber(ARGV[1])

if current < limit then
  redis.call("INCR", KEYS[1])
  redis.call("EXPIRE", KEYS[1], 60)
  return 1
else
  return 0
end
`;

/**
 * Acquire a user slot (blocking with retry)
 */
async function acquireUserSlot(userId: string, limit: number): Promise<void> {
  while (true) {
    try {
      const ok = await connection.eval(
        ACQUIRE_USER_SLOT_LUA,
        1,
        USER_ACTIVE_KEY(userId),
        limit
      );

      if (ok === 1) return;
    } catch (e: any) {
      const msg = e?.message || '';
      logger.error('acquireUserSlot Redis error', { key: USER_ACTIVE_KEY(userId), error: e });
      if (msg.includes('not an integer') || msg.includes('out of range')) {
        try {
          logger.warn('Resetting user active key to 0 due to invalid value', { key: USER_ACTIVE_KEY(userId) });
          await connection.set(USER_ACTIVE_KEY(userId), '0');
        } catch (e2) {
          logger.error('Failed to reset user active key', { key: USER_ACTIVE_KEY(userId), error: e2 });
        }
      }
    }

    await new Promise(r => setTimeout(r, 100));
  }
}

/**
 * Release a user slot
 */
async function releaseUserSlot(userId: string): Promise<void> {
  try {
    const val = await connection.decr(USER_ACTIVE_KEY(userId));
    if (val <= 0) await connection.del(USER_ACTIVE_KEY(userId));
  } catch {}
}

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
    try {
      const ok = await connection.eval(
        ACQUIRE_CPU_LUA,
        1,
        CPU_KEY,
        GLOBAL_CPU_LIMIT
      );

      if (ok === 1) return;
    } catch (e: any) {
      const msg = e?.message || '';
      logger.error('acquireCpuSlot Redis error', { key: CPU_KEY, error: e });
      if (msg.includes('not an integer') || msg.includes('out of range')) {
        try {
          logger.warn('Resetting CPU key to 0 due to invalid value', { key: CPU_KEY });
          await connection.set(CPU_KEY, '0');
        } catch (e2) {
          logger.error('Failed to reset CPU key', { key: CPU_KEY, error: e2 });
        }
      }
    }

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
  try {
    const ok = await connection.eval(
      RESERVE_DISK_LUA,
      1,
      DISK_KEY,
      free,
      bytes
    );

    return ok === 1;
  } catch (e: any) {
    // Recover from corrupted numeric value in Redis (e.g., non-integer stored)
    const msg = e?.message || '';
    logger.error('Redis error during reserveDisk', { error: e, free, bytes });

    if (msg.includes('not an integer') || msg.includes('out of range')) {
      try {
        logger.warn('Resetting disk reservation key to 0 to recover from invalid value', { key: DISK_KEY });
        await connection.set(DISK_KEY, '0');
        const ok2 = await connection.eval(
          RESERVE_DISK_LUA,
          1,
          DISK_KEY,
          free,
          bytes
        );
        return ok2 === 1;
      } catch (e2) {
        logger.error('Retry reserveDisk failed', { error: e2 });
        return false;
      }
    }

    return false;
  }
}

/**
 * Release reserved disk
 */
async function releaseDisk(bytes: number): Promise<void> {
  try {
    const val = await connection.decrby(DISK_KEY, bytes);
    if (val <= 0) await connection.del(DISK_KEY);
  } catch (e) {
    logger.error('releaseDisk failed', { error: e, key: DISK_KEY, bytes });
  }
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
    ...safeExtra,
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
      ...safeExtra,
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
  const normalizedDurationMs =
    Number.isFinite(durationMs) && durationMs > 0
      ? durationMs
      : 5 * 60 * 1000;

  return Math.ceil(Math.max(
    normalizedDurationMs * MULTIPLIER + BUFFER,
    30 * 60 * 1000 // minimum 30 min
  ));
}

async function materializeInputPath(
  rawInputPath: string,
  job: FileJob
): Promise<{ inputPath: string; detectedExt: string | null; detectedMime: string | null }> {
  const detected = await fileTypeFromFile(rawInputPath).catch(() => undefined);
  const detectedExt = normalizeExtension(detected?.ext);

  if (!detectedExt) {
    return {
      inputPath: rawInputPath,
      detectedExt,
      detectedMime: detected?.mime ?? null,
    };
  }

  const resolvedPath = path.join(path.dirname(rawInputPath), `input${detectedExt}`);

  if (resolvedPath !== rawInputPath) {
    await fs.promises.rm(resolvedPath, { force: true });
    await fs.promises.rename(rawInputPath, resolvedPath);
  }

  return {
    inputPath: resolvedPath,
    detectedExt,
    detectedMime: detected?.mime ?? null,
  };
}

/**
 * notifyCallback sends a POST request to the job's callback URL with the provided payload. It includes the job ID, file ID, and any additional information in the payload. The function also handles errors gracefully, logging any issues that occur during the callback attempt. This allows external services to be notified of job completion, progress updates, or failures in a standardized way.
 * @param job - The FileJob object containing metadata and callback information
 * @param payload - An object containing additional data to include in the callback request
 * The payload will be merged with the job's fileId and fileVersionId to provide context to the callback receiver.
 * If the job does not have a callback URL defined, the function will simply return without attempting to send a request.
 * Errors during the fetch operation are caught and logged, but do not throw exceptions to ensure that the main job processing flow is not disrupted by callback issues.
 * This function is essential for integrating the file processing system with external workflows, allowing for real-time notifications and updates based on job status changes.
 * Example usage:
 * notifyCallback(job, { status: 'completed', outputUrl: 'https://example.com/output.mp4' });
 * This would send a POST request to the job's callback URL with a JSON body containing the job ID, file ID, file version ID, status, and output URL.
 * The callback receiver can then use this information to take appropriate actions, such as updating a user interface, triggering additional processing, or sending notifications to users.
 * Overall, this function provides a flexible and robust mechanism for communicating job outcomes to external systems in a decoupled manner.
 * Note: Ensure that the callback URL is secure and that the callback token is used to authenticate requests to prevent unauthorized access.
 * The callback payload can be extended to include any relevant information about the job or its output, making it a powerful tool for integrating with various services and workflows.
 * The function is designed to be resilient, ensuring that even if the callback fails, it does not impact the main processing of the job, while still providing valuable logging for troubleshooting callback issues.
 * In summary, notifyCallback is a critical component for enabling communication between the file processing system and external services, allowing for dynamic and responsive workflows based on job events.
 * @param job - The FileJob object containing metadata and callback information
 * @param payload - An object containing additional data to include in the callback request
 * The payload will be merged with the job's fileId and fileVersionId to provide context to the callback receiver.
 * If the job does not have a callback URL defined, the function will simply return without attempting to send a request.
 * Errors during the fetch operation are caught and logged, but do not throw exceptions to ensure that the main job processing flow is not disrupted by callback issues.
 */
async function notifyCallback(job: FileJob, payload: Record<string, any>) {
  if (!job.callbackUrl) return;

  await fetch(job.callbackUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.PROCESSING_CALLBACK_TOKEN || job.callbackToken || ""}`,
    },
    body: JSON.stringify({
      jobId: job.fileId,
      fileId: payload.fileId ?? job.fileId,
      fileVersionId: job.fileVersionId,
      ...payload,
    }),
  }).catch((error) => {
    logger.error("Processing callback failed", {
      jobId: job.fileId,
      error,
    });
  });
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
  if (!SAFE_JOB_ID.test(job.fileId)) {
    throw new Error('Invalid job id');
  }

  /**
   * Use controlled temp directory instead of OS temp
   * Prevents hidden temp files and debugging issues
   */
  const tempDir = path.join(config.tempDir, job.fileId);
  const rawInputPath = path.join(tempDir, 'input');
  const outputBase = path.join(tempDir, 'output');
  const lockKey = REDIS_KEYS.LOCK(job.fileId);
  const jobKey = REDIS_KEYS.JOB(job.fileId);
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
    logger.info('Skipping duplicate execution', { jobId: job.fileId });
    return;
  }
  logger.info('LOCK ACQUIRED', { jobId: job.fileId, worker: WORKER_ID });

  logger.info('Job started', { jobId: job.fileId, userId: job.userId, fileType: job.fileType });
  const userId = job.userId || 'local-user';
  const startTime = Date.now();
  let heartbeat: NodeJS.Timeout | null = null;
  let userSlotAcquired = false;
  let diskReserved = false;
  let reservedDisk = 0;
  let cpuSlotHeld = false;
  let outputPath = '';
  let safeInput = rawInputPath;
  let videoDurationMs: number | null = null;
  let jobStatus: JobStatus = JOB_STATUS.PROCESSING;

  const acquireLocalCpuSlot = async () => {
    if (cpuSlotHeld) return;
    await acquireCpuSlot();
    cpuSlotHeld = true;
  };

  const releaseLocalCpuSlot = async () => {
    if (!cpuSlotHeld) return;
    await releaseCpuSlot();
    cpuSlotHeld = false;
  };

  const withCpuSlot = async <T>(task: () => Promise<T>): Promise<T> => {
    await acquireLocalCpuSlot();
    try {
      return await task();
    } finally {
      await releaseLocalCpuSlot();
    }
  };

  try {
    /**
     * PER-USER LIMIT
     */
    const limit = config.userLimits[job.userTier] || 2;

    await acquireUserSlot(userId, limit);
    userSlotAcquired = true;

    /**
     * STEP 3: Prepare temp dir BEFORE download
     */
    await fs.promises.mkdir(tempDir, { recursive: true });

    /**
     * Ensure job has initial state without overwriting enqueue metadata
     */
    await connection.hset(jobKey, {
      status: JOB_STATUS.PROCESSING,
      stage: JOB_STAGE.STARTING,
      startedAt: startTime,
    });

    /** Stage 1: Downloading the input file */
    await updateJobStage(job.fileId, jobStatus, JOB_STAGE.DOWNLOADING, {
      startedAt: startTime,
      bullJob,
    });

    /**
     * STEP 4: Download input to local temp path
     */
    if (job.sourceBucket && job.sourceKey) {
      await downloadFromR2({
        bucket: job.sourceBucket,
        key: job.sourceKey,
        dest: rawInputPath,
      });
    } else if (job.inputUrl) {
      await download(job.inputUrl, rawInputPath);
    } else {
      throw new Error("Missing source object.");
    }

    const materializedInput = await materializeInputPath(rawInputPath, job);
    safeInput = materializedInput.inputPath;
    const declaredInputValue =
      job.sourceKey ??
      job.inputUrl ??
      job.originalName ??
      job.outputKey;

    assertDetectedMediaMatchesDeclaration(
      job.fileType,
      declaredInputValue,
      materializedInput.detectedMime,
      materializedInput.detectedExt,
    );
    await assertBasicFileHeader(
      safeInput,
      materializedInput.detectedMime,
      materializedInput.detectedExt
    );

    const downloadedSize = (await fs.promises.stat(safeInput)).size;

    if (downloadedSize > job.size || downloadedSize > config.security.maxUploadBytes) {
      throw new Error('Downloaded file exceeds allowed size');
    }

    /**
     * Probe the downloaded asset so we can fail early on obviously invalid inputs
     * and make container normalization decisions without depending on the temp filename.
     */
    let videoProbe:
      | ReturnType<typeof inspectVideoInput>
      | null = null;

    if (job.fileType === FILE_TYPE.VIDEO) {
      try {
        videoProbe = inspectVideoInput(safeInput);
      } catch (probeErr) {
        logger.warn('VIDEO PROBE FAILED', { jobId: job.fileId, error: probeErr });
      }

      if (!videoProbe) {
        throw new Error('Unable to inspect video metadata');
      }

      if (!videoProbe.hasVideo) {
        throw new Error('Input file does not contain a video stream');
      }

      /**
       * Remux Matroska inputs when possible, but never make that a hard dependency.
       * Some MKV files contain codecs that cannot be copied into MP4 cleanly.
       */
      const isMatroskaInput = isLikelyMatroska({
        formatName: videoProbe?.formatName,
        mime: materializedInput.detectedMime,
        ext: materializedInput.detectedExt,
      });

      if (isMatroskaInput) {
        const remuxed = path.join(tempDir, 'remux.mp4');

        try {
          await withCpuSlot(
            () =>
              new Promise<void>((resolve, reject) => {
                const ff = spawn(config.ffmpegPath, [
                  '-y',
                  '-nostdin',
                  ...LOCAL_PROTOCOL_ARGS,
                  '-fflags', '+genpts',
                  '-i', safeInput,
                  '-map', '0:v:0',
                  '-map', '0:a:0?',
                  '-sn',
                  '-dn',
                  '-c', 'copy',
                  remuxed,
                ]);

                ff.on('close', (code) => {
                  if (code === 0) resolve();
                  else reject(new Error(`MKV remux failed with code ${code}`));
                });
                ff.on('error', reject);
              })
          );

          safeInput = remuxed;

          try {
            videoProbe = inspectVideoInput(safeInput);
          } catch (probeErr) {
            logger.warn('REMUX PROBE FAILED, keeping remuxed input', { jobId: job.fileId, error: probeErr });
          }
        } catch (remuxErr) {
          logger.warn('MKV REMUX SKIPPED, falling back to direct decode', { jobId: job.fileId, error: remuxErr });
        }
      }

      if (!videoProbe) {
        throw new Error('Unable to inspect video metadata');
      }

      assertAllowedMediaInput(job.fileType, materializedInput.detectedMime, declaredInputValue);
      if (!videoProbe.hasVideo) {
        throw new Error('Input file does not contain a video stream');
      }
      assertAllowedVideoProbe(videoProbe);
    }

    /**
     * Calculate required disk:
     * - 2x file size (input + output)
     * - minimum 500MB safety buffer
     */
    reservedDisk = Math.max(job.size * 2, 500 * 1024 * 1024);

    /**
     * Persist exact reservation value.
     * CRITICAL: This must be used during release to prevent drift.
     */
    await connection.hset(jobKey, {
      reservedDisk,
    });

    /**
     * Attempt to reserve disk atomically
     */
    diskReserved = await reserveDisk(reservedDisk);

    if (!diskReserved) {
      throw new Error('Global disk reservation failed (insufficient space)');
    }

    /**
     * STEP 5: Compute accurate TTL AFTER download
     */
    let ttl = computeFileTTL(job.size);

    if (job.fileType === FILE_TYPE.VIDEO) {
      /**
       * SAFELY extract duration after any container normalization
       */
      videoDurationMs = videoProbe?.durationMs ?? 5 * 60 * 1000;

      ttl = computeVideoTTL(videoDurationMs);
    }

    const lockTtlMs = Number.isFinite(ttl)
      ? Math.max(Math.ceil(ttl), LOCK_TTL)
      : LOCK_TTL;

    await connection.pexpire(lockKey, lockTtlMs);

    // HEARTBEAT
    heartbeat = setInterval(async () => {
      try {
        const owner = await connection.get(lockKey);

        if (owner === WORKER_ID) {
          await connection.pexpire(lockKey, LOCK_TTL);

          await connection.hset(jobKey, {
            status: JOB_STATUS.PROCESSING,
            updatedAt: Date.now(),
            heartbeatAt: Date.now(),
          });
        } else if (heartbeat) {
          logger.warn('Lost lock ownership', { jobId: job.fileId });
          clearInterval(heartbeat);
          heartbeat = null;
        }
      } catch (err) {
        logger.error('Heartbeat error', { jobId: job.fileId, error: err });
      }
    }, 60_000);

    /** Stage 2: Processing */
    await updateJobStage(job.fileId, JOB_STATUS.PROCESSING, JOB_STAGE.PROCESSING);

    if (job.fileType === FILE_TYPE.IMAGE) {
      const result = await withCpuSlot(() => processImage(safeInput, outputBase));
      outputPath = result.outputPath;

      job.outputKey = job.outputKey.replace(/\.\w+$/, result.ext);
      await updateJobStage(job.fileId, JOB_STATUS.PROCESSING, JOB_STAGE.PROCESSING, {
        progress: 100,
        bullJob,
      });
    } else if (job.fileType === FILE_TYPE.PDF) {
      const result = await withCpuSlot(() => processPdf(safeInput, outputBase));
      outputPath = result.outputPath;

      job.outputKey = job.outputKey.replace(/\.\w+$/, result.ext);
      await updateJobStage(job.fileId, JOB_STATUS.PROCESSING, JOB_STAGE.PROCESSING, {
        progress: 100,
        bullJob,
      });
    } else if (job.fileType === FILE_TYPE.VIDEO) {
      /**
       * Full watermarked video processing.
       *
       * In Mitfloww, this is the customer preview:
       * - full duration
       * - watermarked
       * - uploaded as processed file
       * - shown to client for review/update requests
       */
      const finalOutput = `${outputBase}.mp4`;
      let lastProgress = -1;
      let lastProgressAt = 0;

      await withCpuSlot(() =>
        processVideo(
          safeInput,
          finalOutput,
          videoDurationMs
            ? { totalDuration: videoDurationMs, jobId: job.fileId }
            : { jobId: job.fileId },
          (progress) => {
            const normalized = Math.max(0, Math.min(Math.round(progress), 100));
            const now = Date.now();

            if (normalized < 100) {
              if (normalized <= lastProgress) return;
              if (now - lastProgressAt < 750) return;
            }

            lastProgress = normalized;
            lastProgressAt = now;

            void updateJobStage(
              job.fileId,
              JOB_STATUS.PROCESSING,
              JOB_STAGE.PROCESSING,
              {
                progress: normalized,
                bullJob,
              },
            ).catch((progressErr) => {
              logger.error("Progress update failed", {
                jobId: job.fileId,
                error: progressErr,
              });
            });
          },
        )
      );

      await updateJobStage(job.fileId, JOB_STATUS.PROCESSING, JOB_STAGE.PROCESSING, {
        progress: 100,
        bullJob,
      });

      outputPath = finalOutput;
    }

    /** Stage 3: Uploading the processed file */
    await updateJobStage(job.fileId, JOB_STATUS.PROCESSING, JOB_STAGE.UPLOADING);
    /**
     * Wait for file to be fully written before upload (important for videos)
     */
    let result: {
        bucket?: string;
        key?: string;
        sizeBytes?: number;
        contentType?: string;
      } | string;

      if (job.outputBucket) {
        const uploaded = await uploadToR2({
          bucket: job.outputBucket,
          key: job.outputKey,
          filePath: outputPath,
        });

        result = uploaded;
      } else {
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

    const processedResult =
      typeof result === "string"
        ? {
            bucket: job.outputBucket || process.env.R2_BUCKET_NAME || "",
            key: job.outputKey,
            mimeType: job.mimeType || "application/octet-stream",
            extension: path.extname(job.outputKey),
            sizeBytes: outputPath ? (await fs.promises.stat(outputPath)).size : 0,
          }
        : {
            bucket: result.bucket || job.outputBucket || "",
            key: result.key || job.outputKey,
            mimeType: result.contentType || job.mimeType || "application/octet-stream",
            extension: path.extname(result.key || job.outputKey),
            sizeBytes: result.sizeBytes || 0,
          };

    let logObject: { bucket: string; key: string } | null = null;

    if (job.outputBucket && job.logKey) {
      const logsRaw = await connection.lrange(REDIS_KEYS.JOB_LOGS(job.fileId), 0, -1);
      const logs = logsRaw.map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return { raw: line };
        }
      });

      logObject = await uploadJsonToR2({
        bucket: job.outputBucket,
        key: job.logKey,
        payload: {
          jobId: job.fileId,
          fileVersionId: job.fileVersionId,
          logs,
        },
      });
    }

    await notifyCallback(job, {
      status: "completed",
      processed: processedResult,
      log: logObject,
    });

  } catch (err: any) {
    logger.error('JOB FAILED', { jobId: job.fileId, error: err, attempts: bullJob?.attemptsMade });
    // Determine if the job can still be retried
    const isRetrying = bullJob && bullJob.attemptsMade < (bullJob.opts.attempts || 1);
    jobStatus = isRetrying ? JOB_STATUS.RETRYING : JOB_STATUS.FAILED;
    const MAX_TOTAL_RETRIES = 5;

    /** Update failed state in Redis */
    await updateJobStage(job.fileId, jobStatus, JOB_STAGE.FAILED, {
      error: toPublicErrorMessage(err?.message),
      failedAt: Date.now(),
      attemptsMade: bullJob?.attemptsMade || 0,
      maxAttempts: bullJob?.opts.attempts || 1,
      success: false,
      bullJob,
    });

    if (!isRetrying) {
      await notifyCallback(job, {
        status:
          toPublicErrorMessage(err?.message) === "Invalid or unsupported media file"
            ? "corrupt"
            : "failed",
        errorCode: "processing_failed",
        errorMessage: toPublicErrorMessage(err?.message),
      });
    }

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
        logger.warn('Poison job detected', { jobId: job.fileId });

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
        logger.info('Requeueing job', { jobId: job.fileId, attempt: totalRetries + 1 });

        await enqueueFile({
          ...job,
          retryCount: totalRetries + 1,
        });
      } else {
        logger.info('Moving job to DLQ', { jobId: job.fileId });
        await connection.rpush(REDIS_KEYS.DLQ, JSON.stringify(job));
      }
    }

    // Bubble up the error for BullMQ to handle retry/backoff
    throw err;

  } finally {
    await releaseLocalCpuSlot();

    /**
     * Release user slot
     */
    if (userSlotAcquired) {
      try {
        await releaseUserSlot(userId);
      } catch {
        // swallow — cleanup must never crash worker
      }
    }

    /**
     * Release EXACT reserved disk.
     * Prevents drift between reserve/release values.
     */
    if (diskReserved && reservedDisk > 0) {
      try {
        await releaseDisk(reservedDisk);
      } catch {
        // swallow — cleanup must never crash worker
      }
    }

    /**
     * Cleanup strategy:
     * - Always clean on success
     * - Keep failed temp for retry BUT limit retention
     */
    if (jobStatus === JOB_STATUS.COMPLETED) {
      await fs.promises.rm(tempDir, { recursive: true, force: true });
    } else {
      /**
       * Schedule delayed cleanup for failed jobs to allow for retries and debugging.
       * Prevent disk explosion
       */
      setTimeout(async () => {
        try {
            await fs.promises.rm(tempDir, { recursive: true, force: true });
          } catch (cleanupErr) { 
            logger.error('Cleanup deletion failed', { jobId: job.fileId, error: cleanupErr });
          }
      }, 1000 * 60 * 30); // 30 minutes retention
    }

    // Release lock
    try {
      const owner = await connection.get(lockKey);
      if (owner === WORKER_ID) {
        await connection.del(lockKey);
      }
    } catch (err) {
      logger.error('Lock release error', { jobId: job.fileId, error: err });
    }
    if (heartbeat) {
      clearInterval(heartbeat);
    }
  }
}
