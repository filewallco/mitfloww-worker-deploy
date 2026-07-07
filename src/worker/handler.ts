import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import { DelayedError, Job, UnrecoverableError } from "bullmq";
import { fileTypeFromFile } from "file-type";
import { config } from "../config";
import { FILE_TYPE, JOB_STAGE, JOB_STATUS, REDIS_KEYS } from "../constants";
import { connection } from "../queue/connection";
import { classify } from "../queue/priority";
import { processImage } from "../processors/image";
import { processPdf } from "../processors/pdf";
import {
  assertAllowedVideoProbe,
  inspectVideoInput,
  processVideo,
} from "../processors/video";
import { toPublicErrorMessage } from "../security/errors";
import { FileJob, JobStatus } from "../types";
import { assertBasicFileHeader } from "../utils/fileSignature";
import { logger } from "../utils/logger";
import {
  assertAllowedMediaInput,
  assertDetectedMediaMatchesDeclaration,
  isLikelyMatroska,
  normalizeExtension,
} from "../utils/media";
import {
  download,
  downloadFromR2,
  headR2Object,
  upload,
  uploadJsonToR2,
  uploadToR2,
} from "../utils/r2";
import { recordProcessingDuration } from "../utils/eta";
import { buildTraceableWatermarkText } from "../utils/watermark";
import {
  CorruptInputError,
  DiskUnavailableError,
  DuplicateActiveFileVersionError,
  ResourceUnavailableError,
  ResourceWaitTimeoutError,
  SourceMissingError,
  TransientProcessingError,
  WorkerCapacityExceededError,
  WorkerError,
} from "./errors";
import {
  canEverFitJob,
  estimateRequiredDisk,
  releaseCpuLane,
  releaseDisk,
  releaseUserSlot,
  refreshReservations,
  tryAcquireCpuLane,
  tryAcquireUserSlot,
  tryReserveDisk,
  type CpuLane,
} from "./resourceManager";

const SAFE_JOB_ID = /^[A-Za-z0-9_-]{1,128}$/;
const LOCK_TTL_MS = 60 * 60 * 1000;
const FILE_VERSION_KEY_TTL_MS = 24 * 60 * 60 * 1000;
const LOCAL_PROTOCOL_ARGS = ["-safe", "1", "-protocol_whitelist", "file,pipe"];
const WORKER_ID = `${process.pid}-${Date.now()}`;

type HandleContext = {
  bullJob?: Job<FileJob>;
  token?: string;
  startTime: number;
  jobKey: string;
};

type SourceMetadata = {
  expectedBytes: number | null;
  sourceDescription: string;
};

function parseNumber(value: string | undefined): number | null {
  if (!value) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function isLikelyCorruptError(message: string): boolean {
  const normalized = message.toLowerCase();
  return (
    normalized.includes("invalid") ||
    normalized.includes("unsupported") ||
    normalized.includes("codec") ||
    normalized.includes("file signature") ||
    normalized.includes("mime and extension mismatch") ||
    normalized.includes("does not contain a video stream") ||
    normalized.includes("malformed") ||
    normalized.includes("encrypted")
  );
}

function isNoSpaceError(error: unknown): boolean {
  const anyErr = error as any;
  const message = String(anyErr?.message || "");
  return anyErr?.code === "ENOSPC" || /no space left on device/i.test(message);
}

function maxAttempts(bullJob?: Job<FileJob>): number {
  const attempts = Number(
    bullJob?.opts?.attempts ?? config.processing.maxAttempts,
  );
  return Number.isFinite(attempts) && attempts > 0
    ? attempts
    : config.processing.maxAttempts;
}

async function updateJobStage(
  jobId: string,
  status: JobStatus,
  stage: string,
  extra?: Record<string, unknown>,
) {
  const { bullJob, ...safeExtra } = extra || {};
  const job = bullJob as Job<FileJob> | undefined;
  const progress = safeExtra.progress;

  if (job && typeof progress === "number") {
    await job.updateProgress(progress);
  }

  const payload = {
    status,
    stage,
    ...safeExtra,
    updatedAt: Date.now(),
  };

  await connection.hset(REDIS_KEYS.JOB(jobId), payload);
  await connection.rpush(
    REDIS_KEYS.JOB_LOGS(jobId),
    JSON.stringify({
      time: Date.now(),
      status,
      stage,
      ...safeExtra,
    }),
  );
  await connection.expire(REDIS_KEYS.JOB(jobId), 60 * 60 * 24);
  await connection.expire(REDIS_KEYS.JOB_LOGS(jobId), 60 * 60 * 24);
}

async function notifyCallback(job: FileJob, payload: Record<string, unknown>) {
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
    logger.error("Processing callback failed", { jobId: job.fileId, error });
  });
}

async function materializeInputPath(
  rawInputPath: string,
): Promise<{
  inputPath: string;
  detectedExt: string | null;
  detectedMime: string | null;
}> {
  const detected = await fileTypeFromFile(rawInputPath).catch(() => undefined);
  const detectedExt = normalizeExtension(detected?.ext);

  if (!detectedExt) {
    return {
      inputPath: rawInputPath,
      detectedExt: null,
      detectedMime: detected?.mime ?? null,
    };
  }

  const resolvedPath = path.join(
    path.dirname(rawInputPath),
    `input${detectedExt}`,
  );
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

async function resolveSourceMetadata(job: FileJob): Promise<SourceMetadata> {
  if (job.sourceBucket && job.sourceKey) {
    const head = await headR2Object({
      bucket: job.sourceBucket,
      key: job.sourceKey,
    });
    if (!head) {
      throw new SourceMissingError("Source object not found in R2");
    }

    if (
      !Number.isFinite(head.contentLength) ||
      (head.contentLength as number) <= 0
    ) {
      throw new SourceMissingError("Source object size is unavailable");
    }

    const expectedBytes = Number(head.contentLength);
    if (expectedBytes > config.security.maxUploadBytes) {
      throw new WorkerCapacityExceededError(
        "capacity_exceeded",
        `Source object exceeds max upload size: ${expectedBytes}`,
        "File is too large for the current worker capacity",
      );
    }

    return {
      expectedBytes,
      sourceDescription: `r2://${job.sourceBucket}/${job.sourceKey}`,
    };
  }

  if (job.inputUrl) {
    if (!Number.isFinite(job.size) || job.size <= 0) {
      throw new SourceMissingError("Input size is missing for URL source");
    }

    return {
      expectedBytes: job.size,
      sourceDescription: job.inputUrl,
    };
  }

  throw new SourceMissingError("Missing source object");
}

function normalizeError(error: unknown): Error {
  if (error instanceof WorkerError) return error;
  if (error instanceof DelayedError) return error;
  if (error instanceof UnrecoverableError) return error;

  const anyErr = error as any;
  const message = String(anyErr?.message || "Processing failed");

  if (isNoSpaceError(error)) {
    return new DiskUnavailableError();
  }

  if (
    message.includes("Download failed: 404") ||
    /source object not found/i.test(message) ||
    /nosuchkey/i.test(message)
  ) {
    return new SourceMissingError(message);
  }

  if (
    message.includes("maximum upload size") ||
    message.includes("allowed size") ||
    message.includes("exceeds expected content length")
  ) {
    return new WorkerCapacityExceededError(
      "capacity_exceeded",
      message,
      "File is too large for the current worker capacity",
    );
  }

  if (isLikelyCorruptError(message)) {
    return new CorruptInputError(message);
  }

  return new TransientProcessingError(message);
}

async function scheduleResourceWait(
  job: FileJob,
  waitError: ResourceUnavailableError,
  context: HandleContext,
) {
  const now = Date.now();
  const meta = await connection.hgetall(context.jobKey);

  const previousFirstWaitAt = parseNumber(meta.firstResourceWaitAt);
  const firstWaitAt =
    previousFirstWaitAt && previousFirstWaitAt > 0 && previousFirstWaitAt <= now
      ? previousFirstWaitAt
      : now;

  const previousResourceWaitCount = parseNumber(meta.resourceWaitCount);
  const resourceWaitCount =
    previousFirstWaitAt &&
    previousFirstWaitAt > 0 &&
    previousResourceWaitCount &&
    previousResourceWaitCount > 0
      ? previousResourceWaitCount + 1
      : 1;

  const waitedMs = Math.max(0, now - firstWaitAt);

  if (
    resourceWaitCount > config.processing.maxResourceWaitCount ||
    waitedMs > config.processing.maxResourceWaitMs
  ) {
    throw new ResourceWaitTimeoutError();
  }

  const nextRetryAt = now + config.processing.resourceRetryDelayMs;
  const nextStatus =
    (context.bullJob?.attemptsMade ?? 0) > 0
      ? JOB_STATUS.RETRYING
      : JOB_STATUS.QUEUED;

  await updateJobStage(job.fileId, nextStatus, waitError.stage, {
    waitReason: waitError.waitReason,
    resourceWaitCount,
    firstResourceWaitAt: firstWaitAt,
    nextRetryAt,
    attemptsMade: context.bullJob?.attemptsMade ?? 0,
    maxAttempts: maxAttempts(context.bullJob),
    bullJob: context.bullJob,
  });

  if (job.fileVersionId) {
    const queuedKey = REDIS_KEYS.QUEUED_FILE_VERSION(job.fileVersionId);
    const queuedOwner = await connection.get(queuedKey);
    if (queuedOwner === job.fileId) {
      await connection.pexpire(queuedKey, FILE_VERSION_KEY_TTL_MS);
    }
  }

  if (context.bullJob && context.token) {
    await context.bullJob.moveToDelayed(nextRetryAt, context.token);
    throw new DelayedError("resource_wait");
  }

  throw waitError;
}

async function updateFailedState(
  job: FileJob,
  err: WorkerError | Error,
  context: HandleContext,
  failureStatus: JobStatus,
) {
  const publicMessage =
    err instanceof WorkerError
      ? err.publicMessage
      : toPublicErrorMessage(err.message);
  const errorCode =
    err instanceof WorkerError ? err.code : "transient_processing_failure";

  await updateJobStage(job.fileId, failureStatus, JOB_STAGE.FAILED, {
    error: publicMessage,
    errorCode,
    failedAt: Date.now(),
    attemptsMade: context.bullJob?.attemptsMade ?? 0,
    maxAttempts: maxAttempts(context.bullJob),
    success: false,
    bullJob: context.bullJob,
  });
}

async function removeTempForResourceWait(
  jobId: string,
  jobKey: string,
  tempDir: string,
): Promise<boolean> {
  try {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
    await connection.hdel(jobKey, "tempDir", "tempCleanupEligibleAt");
    logger.info("Removed temp dir for resource wait", { jobId, tempDir });
    return true;
  } catch (error) {
    logger.error("Failed to remove temp dir for resource wait", {
      jobId,
      tempDir,
      error,
    });
    return false;
  }
}

export async function handleJob(
  job: FileJob,
  bullJob?: Job<FileJob>,
  token?: string,
) {
  if (!SAFE_JOB_ID.test(job.fileId)) {
    throw new Error("Invalid job id");
  }

  if (!Number.isFinite(job.size) || job.size <= 0) {
    throw new Error("Invalid job size");
  }

  const startTime = Date.now();
  const tempDir = path.join(config.tempDir, job.fileId);
  const rawInputPath = path.join(tempDir, "input");
  const outputBase = path.join(tempDir, "output");
  const lockKey = REDIS_KEYS.LOCK(job.fileId);
  const jobKey = REDIS_KEYS.JOB(job.fileId);
  const context: HandleContext = { bullJob, token, startTime, jobKey };
  const userId = job.userId || "local-user";
  const cpuLane: CpuLane = (() => {
    if (job.fileType === FILE_TYPE.IMAGE || job.fileType === FILE_TYPE.PDF) {
      return "image";
    }

    if (job.fileType === FILE_TYPE.VIDEO) {
      const sizeType = classify(job.size);

      if (sizeType === "small") return "small";
      if (sizeType === "medium") return "medium";
      return "heavy";
    }

    return "small";
  })();
  const watermarkText = buildTraceableWatermarkText({
    userEmail: job.userEmail,
    userName: job.userName,
    fileVersionId: job.fileVersionId,
  });

  let heartbeat: NodeJS.Timeout | null = null;
  let jobStatus: JobStatus = JOB_STATUS.PROCESSING;
  let diskReserved = false;
  let userSlotHeld = false;
  let fileVersionSlotHeld = false;
  let clearQueuedFileVersionIndex = false;
  let outputPath = "";
  let safeInput = rawInputPath;
  let videoDurationMs: number | null = null;
  let expectedSourceBytes: number | null = null;
  let tempDirCreated = false;
  let cpuHeavyTaskActive = false;
  let tempRemovedForResourceWait = false;

  const withCpuSlot = async <T>(task: () => Promise<T>): Promise<T> => {
    const acquired = await tryAcquireCpuLane(job.fileId, cpuLane);

    if (!acquired) {
      throw new ResourceUnavailableError(
        `${cpuLane} CPU slot unavailable`,
        JOB_STAGE.WAITING_FOR_CPU,
        `${cpuLane}_cpu_slot_unavailable`,
      );
    }

    try {
      cpuHeavyTaskActive = true;
      return await task();
    } finally {
      cpuHeavyTaskActive = false;

      try {
        await releaseCpuLane(job.fileId, cpuLane);
      } catch (error) {
        logger.error("Failed to release CPU slot", {
          jobId: job.fileId,
          cpuLane,
          error,
        });
      }
    }
  };

  const lockResult = await connection.set(
    lockKey,
    WORKER_ID,
    "PX",
    LOCK_TTL_MS,
    "NX",
  );
  if (lockResult !== "OK") {
    logger.info("Skipping duplicate execution", { jobId: job.fileId });
    return;
  }

  try {
    if (job.fileVersionId) {
      const activeKey = REDIS_KEYS.ACTIVE_FILE_VERSION(job.fileVersionId);
      const activeSet = await connection.set(
        activeKey,
        job.fileId,
        "PX",
        LOCK_TTL_MS,
        "NX",
      );
      if (activeSet !== "OK") {
        const owner = await connection.get(activeKey);
        if (owner && owner !== job.fileId) {
          throw new DuplicateActiveFileVersionError();
        }
        await connection.pexpire(activeKey, LOCK_TTL_MS);
      }
      fileVersionSlotHeld = true;
    }

    const declaredInputValue =
      job.sourceKey ?? job.inputUrl ?? job.originalName ?? job.outputKey;

    assertAllowedMediaInput(
      job.fileType,
      job.mimeType ?? null,
      declaredInputValue,
    );
    assertAllowedMediaInput(job.fileType, null, job.outputKey);

    const source = await resolveSourceMetadata(job);
    expectedSourceBytes = source.expectedBytes;

    const requiredDisk = estimateRequiredDisk(
      job,
      expectedSourceBytes ?? job.size,
    );
    await connection.hset(jobKey, {
      requiredDisk,
      sourceBytes: expectedSourceBytes ?? job.size,
    });

    if (!canEverFitJob(requiredDisk)) {
      throw new WorkerCapacityExceededError(
        "capacity_exceeded",
        `Job requires ${requiredDisk} bytes but worker cannot fit this size`,
        "File is too large for the current worker capacity",
      );
    }

    diskReserved = await tryReserveDisk(job.fileId, requiredDisk);
    if (!diskReserved) {
      throw new DiskUnavailableError();
    }

    await updateJobStage(
      job.fileId,
      JOB_STATUS.PROCESSING,
      JOB_STAGE.RESERVED,
      {
        requiredDisk,
        sourceBytes: expectedSourceBytes ?? job.size,
        waitReason: "",
        nextRetryAt: "",
        attemptsMade: bullJob?.attemptsMade ?? 0,
        maxAttempts: maxAttempts(bullJob),
        bullJob,
      },
    );

    const userLimit = config.userLimits[job.userTier] || config.userLimits.free;
    userSlotHeld = await tryAcquireUserSlot(userId, job.fileId, userLimit);
    if (!userSlotHeld) {
      throw new ResourceUnavailableError(
        "User slot unavailable",
        JOB_STAGE.WAITING_FOR_USER_SLOT,
        "user_slot_unavailable",
      );
    }

    await fs.promises.mkdir(tempDir, { recursive: true });
    tempDirCreated = true;
    await updateJobStage(
      job.fileId,
      JOB_STATUS.PROCESSING,
      JOB_STAGE.VALIDATING,
      {
        startedAt: startTime,
        heartbeatAt: Date.now(),
        waitReason: "",
        nextRetryAt: "",
        attemptsMade: bullJob?.attemptsMade ?? 0,
        maxAttempts: maxAttempts(bullJob),
        bullJob,
      },
    );

    heartbeat = setInterval(async () => {
      try {
        const owner = await connection.get(lockKey);
        if (owner !== WORKER_ID) return;

        await connection.pexpire(lockKey, LOCK_TTL_MS);
        if (job.fileVersionId) {
          await connection.pexpire(
            REDIS_KEYS.ACTIVE_FILE_VERSION(job.fileVersionId),
            LOCK_TTL_MS,
          );
          const queuedKey = REDIS_KEYS.QUEUED_FILE_VERSION(job.fileVersionId);
          const queuedOwner = await connection.get(queuedKey);
          if (queuedOwner === job.fileId) {
            await connection.pexpire(queuedKey, FILE_VERSION_KEY_TTL_MS);
          }
        }
        await refreshReservations(job.fileId);
        await connection.hset(jobKey, {
          heartbeatAt: Date.now(),
          updatedAt: Date.now(),
        });
      } catch (error) {
        logger.error("Heartbeat error", { jobId: job.fileId, error });
      }
    }, 60_000);

    await updateJobStage(
      job.fileId,
      JOB_STATUS.PROCESSING,
      JOB_STAGE.DOWNLOADING,
      {
        source: source.sourceDescription,
        bullJob,
      },
    );

    if (job.sourceBucket && job.sourceKey) {
      await downloadFromR2({
        bucket: job.sourceBucket,
        key: job.sourceKey,
        dest: rawInputPath,
        expectedBytes: expectedSourceBytes,
        maxBytes: config.security.maxUploadBytes,
      });
    } else if (job.inputUrl) {
      await download(job.inputUrl, rawInputPath, {
        expectedBytes: expectedSourceBytes,
        maxBytes: config.security.maxUploadBytes,
      });
    } else {
      throw new SourceMissingError("Missing source object");
    }

    const materialized = await materializeInputPath(rawInputPath);
    safeInput = materialized.inputPath;

    assertDetectedMediaMatchesDeclaration(
      job.fileType,
      declaredInputValue,
      materialized.detectedMime,
      materialized.detectedExt,
    );
    await assertBasicFileHeader(
      safeInput,
      materialized.detectedMime,
      materialized.detectedExt,
    );

    const downloadedSize = (await fs.promises.stat(safeInput)).size;
    if (downloadedSize > config.security.maxUploadBytes) {
      throw new WorkerCapacityExceededError(
        "capacity_exceeded",
        `Downloaded file exceeds max upload size: ${downloadedSize}`,
      );
    }

    if (expectedSourceBytes && downloadedSize > expectedSourceBytes) {
      throw new WorkerCapacityExceededError(
        "capacity_exceeded",
        `Downloaded bytes exceed expected source size: ${downloadedSize} > ${expectedSourceBytes}`,
      );
    }

    let videoProbe: ReturnType<typeof inspectVideoInput> | null = null;

    if (job.fileType === FILE_TYPE.VIDEO) {
      await withCpuSlot(async () => {
        videoProbe = inspectVideoInput(safeInput);
        assertAllowedVideoProbe(videoProbe!);

        if (!videoProbe!.hasVideo) {
          throw new CorruptInputError(
            "Input file does not contain a video stream",
          );
        }

        const isMatroskaInput = isLikelyMatroska({
          formatName: videoProbe!.formatName,
          mime: materialized.detectedMime,
          ext: materialized.detectedExt,
        });

        if (isMatroskaInput) {
          const remuxed = path.join(tempDir, "remux.mp4");
          try {
            await new Promise<void>((resolve, reject) => {
              const ff = spawn(config.ffmpegPath, [
                "-y",
                "-nostdin",
                ...LOCAL_PROTOCOL_ARGS,
                "-fflags",
                "+genpts",
                "-i",
                safeInput,
                "-map",
                "0:v:0",
                "-map",
                "0:a:0?",
                "-sn",
                "-dn",
                "-c",
                "copy",
                remuxed,
              ]);

              ff.on("close", (code) => {
                if (code === 0) resolve();
                else reject(new Error(`MKV remux failed with code ${code}`));
              });
              ff.on("error", reject);
            });
            safeInput = remuxed;
            videoProbe = inspectVideoInput(safeInput);
          } catch (error) {
            logger.warn("MKV remux skipped; fallback to direct decode", {
              jobId: job.fileId,
              error,
            });
          }
        }

        videoDurationMs = videoProbe!.durationMs ?? null;
      });
    }

    await updateJobStage(
      job.fileId,
      JOB_STATUS.PROCESSING,
      JOB_STAGE.PROCESSING,
      {
        progress: 0,
        waitReason: "",
        nextRetryAt: "",
        bullJob,
      },
    );

    if (job.fileType === FILE_TYPE.IMAGE) {
      const result = await withCpuSlot(() =>
        processImage(safeInput, outputBase, {
          watermarkText,
          compress: job.isLargeFile,
        }),
      );
      outputPath = result.outputPath;
      job.outputKey = job.outputKey.replace(/\.\w+$/, result.ext);
      await updateJobStage(
        job.fileId,
        JOB_STATUS.PROCESSING,
        JOB_STAGE.PROCESSING,
        {
          progress: 100,
          bullJob,
        },
      );
    } else if (job.fileType === FILE_TYPE.PDF) {
      const result = await withCpuSlot(() =>
        processPdf(safeInput, outputBase, {
          watermarkText,
        }),
      );
      outputPath = result.outputPath;
      job.outputKey = job.outputKey.replace(/\.\w+$/, result.ext);
      await updateJobStage(
        job.fileId,
        JOB_STATUS.PROCESSING,
        JOB_STAGE.PROCESSING,
        {
          progress: 100,
          bullJob,
        },
      );
    } else if (job.fileType === FILE_TYPE.VIDEO) {
      const finalOutput = `${outputBase}.mp4`;
      let lastProgress = -1;
      let lastProgressAt = 0;

      await withCpuSlot(() =>
        processVideo(
          safeInput,
          finalOutput,
          {
            totalDuration: videoDurationMs ?? undefined,
            jobId: job.fileId,
            width: videoProbe?.width ?? null,
            height: videoProbe?.height ?? null,
            watermarkText,
            isLargeFile: job.isLargeFile,
          },
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
            ).catch((progressError) => {
              logger.error("Progress update failed", {
                jobId: job.fileId,
                error: progressError,
              });
            });
          },
        ),
      );

      await updateJobStage(
        job.fileId,
        JOB_STATUS.PROCESSING,
        JOB_STAGE.PROCESSING,
        {
          progress: 100,
          bullJob,
        },
      );
      outputPath = finalOutput;
    } else {
      throw new CorruptInputError(`Unsupported file type: ${job.fileType}`);
    }

    await updateJobStage(
      job.fileId,
      JOB_STATUS.UPLOADING,
      JOB_STAGE.UPLOADING,
      {
        bullJob,
      },
    );

    const result = job.outputBucket
      ? await uploadToR2({
          bucket: job.outputBucket,
          key: job.outputKey,
          filePath: outputPath,
          holderId: `${job.fileId}:upload`,
        })
      : await upload(outputPath, job.outputKey, `${job.fileId}:upload`);

    jobStatus = JOB_STATUS.COMPLETED;
    clearQueuedFileVersionIndex = true;
    const durationMs = Date.now() - startTime;
    await updateJobStage(job.fileId, JOB_STATUS.COMPLETED, JOB_STAGE.DONE, {
      completedAt: Date.now(),
      duration: durationMs,
      output: result,
      success: true,
      bullJob,
    });

    await recordProcessingDuration(
      job.fileType,
      expectedSourceBytes ?? job.size,
      durationMs,
    );

    const processedResult =
      typeof result === "string"
        ? {
            bucket: job.outputBucket || process.env.R2_BUCKET_NAME || "",
            key: job.outputKey,
            mimeType: job.mimeType || "application/octet-stream",
            extension: path.extname(job.outputKey),
            sizeBytes: outputPath
              ? (await fs.promises.stat(outputPath)).size
              : 0,
          }
        : {
            bucket: result.bucket || job.outputBucket || "",
            key: result.key || job.outputKey,
            mimeType:
              result.contentType || job.mimeType || "application/octet-stream",
            extension: path.extname(result.key || job.outputKey),
            sizeBytes: result.sizeBytes || 0,
          };

    let logObject: { bucket: string; key: string } | null = null;
    if (job.outputBucket && job.logKey) {
      const logsRaw = await connection.lrange(
        REDIS_KEYS.JOB_LOGS(job.fileId),
        0,
        -1,
      );
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
  } catch (rawError) {
    if (rawError instanceof DelayedError) {
      throw rawError;
    }

    const normalized = normalizeError(rawError);

    if (normalized instanceof ResourceUnavailableError) {
      if (tempDirCreated && !cpuHeavyTaskActive) {
        tempRemovedForResourceWait = await removeTempForResourceWait(
          job.fileId,
          jobKey,
          tempDir,
        );
      }

      try {
        await scheduleResourceWait(job, normalized, context);
      } catch (scheduleError) {
        if (scheduleError instanceof ResourceWaitTimeoutError) {
          jobStatus = JOB_STATUS.FAILED;
          clearQueuedFileVersionIndex = true;
          await updateFailedState(
            job,
            scheduleError,
            context,
            JOB_STATUS.FAILED,
          );
          await notifyCallback(job, {
            status: "failed",
            errorCode: scheduleError.code,
            errorMessage: scheduleError.publicMessage,
          });
          throw new UnrecoverableError(scheduleError.publicMessage);
        }
        throw scheduleError;
      }
      throw new DelayedError("resource_wait");
    }

    if (normalized instanceof ResourceWaitTimeoutError) {
      jobStatus = JOB_STATUS.FAILED;
      clearQueuedFileVersionIndex = true;
      await updateFailedState(job, normalized, context, JOB_STATUS.FAILED);
      await notifyCallback(job, {
        status: "failed",
        errorCode: normalized.code,
        errorMessage: normalized.publicMessage,
      });
      throw new UnrecoverableError(normalized.publicMessage);
    }

    if (
      normalized instanceof WorkerCapacityExceededError ||
      normalized instanceof CorruptInputError ||
      normalized instanceof SourceMissingError ||
      normalized instanceof DuplicateActiveFileVersionError
    ) {
      jobStatus = JOB_STATUS.FAILED;
      clearQueuedFileVersionIndex = true;
      await updateFailedState(job, normalized, context, JOB_STATUS.FAILED);
      await notifyCallback(job, {
        status: normalized instanceof CorruptInputError ? "corrupt" : "failed",
        errorCode: normalized.code,
        errorMessage: normalized.publicMessage,
      });
      throw new UnrecoverableError(normalized.publicMessage);
    }

    const transientError =
      normalized instanceof Error
        ? normalized
        : new TransientProcessingError("Processing failed");
    const shouldRetry = (bullJob?.attemptsMade ?? 0) < maxAttempts(bullJob) - 1;
    jobStatus = shouldRetry ? JOB_STATUS.RETRYING : JOB_STATUS.FAILED;
    clearQueuedFileVersionIndex = !shouldRetry;

    await updateFailedState(job, transientError, context, jobStatus);

    if (!shouldRetry) {
      await notifyCallback(job, {
        status: "failed",
        errorCode: "processing_failed",
        errorMessage: toPublicErrorMessage(transientError.message),
      });
    }

    throw transientError;
  } finally {
    if (heartbeat) clearInterval(heartbeat);

    if (userSlotHeld) {
      try {
        await releaseUserSlot(userId, job.fileId);
      } catch (error) {
        logger.error("Failed to release user slot", {
          jobId: job.fileId,
          error,
        });
      }
    }

    if (diskReserved) {
      try {
        await releaseDisk(job.fileId);
      } catch (error) {
        logger.error("Failed to release disk reservation", {
          jobId: job.fileId,
          error,
        });
      }
    }

    if (clearQueuedFileVersionIndex && job.fileVersionId) {
      try {
        const queuedKey = REDIS_KEYS.QUEUED_FILE_VERSION(job.fileVersionId);
        const owner = await connection.get(queuedKey);
        if (owner === job.fileId) {
          await connection.del(queuedKey);
        }
      } catch (error) {
        logger.error("Failed to release fileVersion queued slot", {
          jobId: job.fileId,
          fileVersionId: job.fileVersionId,
          error,
        });
      }
    }

    if (fileVersionSlotHeld && job.fileVersionId) {
      try {
        const key = REDIS_KEYS.ACTIVE_FILE_VERSION(job.fileVersionId);
        const owner = await connection.get(key);
        if (owner === job.fileId) {
          await connection.del(key);
        }
      } catch (error) {
        logger.error("Failed to release fileVersion active slot", {
          jobId: job.fileId,
          fileVersionId: job.fileVersionId,
          error,
        });
      }
    }

    if (jobStatus === JOB_STATUS.COMPLETED) {
      try {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
        await connection.hdel(jobKey, "tempDir", "tempCleanupEligibleAt");
      } catch (error) {
        logger.error("Completed temp cleanup failed", {
          jobId: job.fileId,
          error,
        });
        try {
          await connection.hset(jobKey, {
            tempDir,
            tempCleanupEligibleAt: Date.now(),
          });
        } catch (metadataError) {
          logger.error("Completed temp cleanup metadata update failed", {
            jobId: job.fileId,
            error: metadataError,
          });
        }
      }
    } else if (tempRemovedForResourceWait) {
      try {
        await connection.hdel(jobKey, "tempDir", "tempCleanupEligibleAt");
      } catch (error) {
        logger.error("Temp cleanup metadata clear failed after resource wait", {
          jobId: job.fileId,
          error,
        });
      }
    } else {
      try {
        await connection.hset(jobKey, {
          tempDir,
          tempCleanupEligibleAt:
            Date.now() + config.cleanup.failedTempRetentionMs,
        });
      } catch (error) {
        logger.error("Temp cleanup metadata update failed", {
          jobId: job.fileId,
          error,
        });
      }
    }

    try {
      const owner = await connection.get(lockKey);
      if (owner === WORKER_ID) {
        await connection.del(lockKey);
      }
    } catch (error) { 
      logger.error("Lock release error", { jobId: job.fileId, error });
    }
  }
}
