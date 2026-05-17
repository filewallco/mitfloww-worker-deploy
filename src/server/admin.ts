import { smallQueue, mediumQueue, largeQueue } from '../queue/queues';
import { connection } from '../queue/connection';
import { enqueueFile } from '../queue/enqueue';
import { imageQueue } from '../queue/queues';
import { FILE_TYPE, JOB_STAGE, JOB_STATUS, QUEUE_NAME, REDIS_KEYS } from "../constants";
import { config } from '../config';
import { logger } from '../utils/logger';
import { getJobReservation, reconcileResourceHolders, releaseDisk } from '../worker/resourceManager';

async function getBullState(jobId: string): Promise<string | null> {
  const jobInstance =
    (await imageQueue.getJob(jobId)) ||
    (await smallQueue.getJob(jobId)) ||
    (await mediumQueue.getJob(jobId)) ||
    (await largeQueue.getJob(jobId));

  if (!jobInstance) return null;
  return await jobInstance.getState();
}
/**
 * Returns structured system snapshot.
 * Separates LIVE jobs and HISTORY jobs.
 */
export async function getSystemSnapshot() {
  const keys = (await scanKeys('job:*')).filter(
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
  const imageCount = await imageQueue.getWaitingCount();

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
    const state = queueState ?? meta.status ?? 'unknown';

    /**
     * Compute queue position based on resolved state
     * Only applies to jobs that are still waiting in queue
     */
    let queuePosition = 0;

    if (queueState === 'waiting') {
      let queueRef =
        job.queueName === QUEUE_NAME.SMALL
          ? smallQueue
          : job.queueName === QUEUE_NAME.MEDIUM
            ? mediumQueue
            : job.queueName === QUEUE_NAME.IMAGE
              ? imageQueue
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
    if (job.queueName === QUEUE_NAME.SMALL) queueSize = smallCount;
    else if (job.queueName === QUEUE_NAME.MEDIUM) queueSize = mediumCount;
    else if (job.queueName === QUEUE_NAME.IMAGE) queueSize = imageCount;
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
      stage: meta.stage || JOB_STAGE.WAITING,
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
    if (state === JOB_STATUS.COMPLETED) {
      stats.completed++;
      history.push(formatted);
    } else if (state === JOB_STATUS.FAILED) {
      stats.failed++;
      history.push(formatted);
    } else {
      live.push(formatted);

      if (
        formatted.stage === JOB_STAGE.WAITING ||
        formatted.stage === JOB_STAGE.WAITING_FOR_DISK ||
        formatted.stage === JOB_STAGE.WAITING_FOR_CPU ||
        formatted.stage === JOB_STAGE.WAITING_FOR_USER_SLOT ||
        formatted.stage === JOB_STAGE.DELAYED
      ) {
        stats.waiting++;
      }
      if (formatted.stage === JOB_STAGE.PROCESSING) stats.processing++;
      if (formatted.stage === JOB_STAGE.UPLOADING) stats.uploading++;
    }

    await connection.hset(`job:${job.id}`, {
      queuePosition,
      queueETA
    });
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
  await reconcileResourceHolders().catch((error) => {
    logger.error('reconcileResourceHolders failed', { error });
  });

  const keys = (await scanKeys("job:*")).filter(
    (key) => !key.includes(":logs"),
  );

  const now = Date.now();
  const STUCK_THRESHOLD = Number(
    process.env.STUCK_JOB_THRESHOLD_MS || 30 * 60 * 1000,
  );
  const RECOVERABLE_STAGES = new Set<string>([
    JOB_STAGE.PROCESSING,
    JOB_STAGE.UPLOADING,
    JOB_STAGE.DOWNLOADING,
    JOB_STAGE.VALIDATING,
    JOB_STAGE.RESERVED,
    JOB_STAGE.WAITING_FOR_DISK,
    JOB_STAGE.WAITING_FOR_CPU,
    JOB_STAGE.WAITING_FOR_USER_SLOT,
    JOB_STAGE.DELAYED,
    JOB_STAGE.STARTING,
    JOB_STAGE.STUCK_RECOVERY,
  ]);

  for (const key of keys) {
    const jobId = key.replace("job:", "");

    try {
      const meta = await connection.hgetall(key);

      const jobInstance =
        (await imageQueue.getJob(jobId)) ||
        (await smallQueue.getJob(jobId)) ||
        (await mediumQueue.getJob(jobId)) ||
        (await largeQueue.getJob(jobId));

      const state = jobInstance ? await jobInstance.getState() : null;
      const lockOwner = await connection.get(REDIS_KEYS.LOCK(jobId));
      const heartbeatAt = Number(meta.heartbeatAt || meta.updatedAt || 0);
      const heartbeatFresh = heartbeatAt > 0 && now - heartbeatAt < STUCK_THRESHOLD;
      const reservationBytes = await getJobReservation(jobId);
      const hasReservation = reservationBytes > 0;

      // Never recover live Bull active jobs.
      if (state === "active") {
        continue;
      }

      // Never recover if lock + heartbeat are still fresh.
      if (lockOwner && heartbeatFresh) {
        continue;
      }

      // Never recover if reservation + heartbeat are still fresh.
      if (hasReservation && heartbeatFresh) {
        continue;
      }

      if (
        state === "waiting" ||
        state === "delayed" ||
        state === "prioritized" ||
        state === "completed" ||
        state === "failed"
      ) {
        continue;
      }

      if (meta.status !== JOB_STATUS.PROCESSING && meta.status !== JOB_STATUS.RETRYING) {
        continue;
      }

      if (meta.stage && !RECOVERABLE_STAGES.has(meta.stage)) {
        continue;
      }

      if (heartbeatFresh) {
        continue;
      }

      const manualRetryCount = Number(meta.manualRetryCount || 0);
      if (manualRetryCount >= config.processing.maxManualRetries) {
        await connection.hset(key, {
          status: JOB_STATUS.FAILED,
          stage: JOB_STAGE.FAILED,
          error: 'Stuck recovery retries exhausted',
          errorCode: 'stuck_recovery_exhausted',
          updatedAt: now,
        });

        if (meta.fileVersionId) {
          const queuedKey = REDIS_KEYS.QUEUED_FILE_VERSION(meta.fileVersionId);
          const activeKey = REDIS_KEYS.ACTIVE_FILE_VERSION(meta.fileVersionId);
          const [queuedOwner, activeOwner] = await Promise.all([
            connection.get(queuedKey),
            connection.get(activeKey),
          ]);
          if (queuedOwner === jobId) {
            await connection.del(queuedKey);
          }
          if (activeOwner === jobId) {
            await connection.del(activeKey);
          }
        }
        continue;
      }

      if (meta.fileVersionId) {
        const activeOwner = await connection.get(REDIS_KEYS.ACTIVE_FILE_VERSION(meta.fileVersionId));
        if (activeOwner && activeOwner !== jobId) {
          continue;
        }
      }

      if (meta.inputUrl?.startsWith('file://') && process.env.ALLOW_LOCAL_FILE_INPUTS !== 'true') {
        continue;
      }

      if (!lockOwner && hasReservation) {
        await releaseDisk(jobId);
      }

      await connection.hset(key, {
        status: JOB_STATUS.RETRYING,
        stage: JOB_STAGE.STUCK_RECOVERY,
        recoveredAt: now,
        manualRetryCount: manualRetryCount + 1,
        updatedAt: now,
      });

      if (jobInstance) {
        try {
          await jobInstance.remove();
        } catch {
          continue;
        }
      }

      const fileType =
        meta.fileType === FILE_TYPE.VIDEO ||
        meta.fileType === FILE_TYPE.IMAGE ||
        meta.fileType === FILE_TYPE.PDF ||
        meta.fileType === FILE_TYPE.ZIP ||
        meta.fileType === FILE_TYPE.OTHER
          ? meta.fileType
          : FILE_TYPE.OTHER;

      const userTier =
        meta.userTier === "free" ||
        meta.userTier === "premium" ||
        meta.userTier === "vip"
          ? meta.userTier
          : "free";

      const size = Number(meta.size);

      if (!Number.isFinite(size) || size <= 0) {
        console.warn(`Skipping stuck job recovery with invalid size: ${jobId}`);
        continue;
      }

      if (!meta.outputKey) {
        console.warn(`Skipping stuck job recovery without outputKey: ${jobId}`);
        continue;
      }

      const hasR2Source = Boolean(meta.sourceBucket && meta.sourceKey);
      const hasAllowedRemoteUrl =
        Boolean(meta.inputUrl) &&
        !meta.inputUrl.startsWith("file://") &&
        process.env.ALLOW_REMOTE_INPUT_URLS === "true";
      const hasAllowedLocalUrl =
        Boolean(meta.inputUrl) &&
        meta.inputUrl.startsWith("file://") &&
        process.env.ALLOW_LOCAL_FILE_INPUTS === "true";

      if (!hasR2Source && !hasAllowedRemoteUrl && !hasAllowedLocalUrl) {
        console.warn(`Skipping stuck job recovery without R2 source: ${jobId}`);
        continue;
      }

      await enqueueFile({
        fileId: jobId,

        inputUrl: hasAllowedRemoteUrl || hasAllowedLocalUrl ? meta.inputUrl : undefined,
        sourceBucket: meta.sourceBucket || undefined,
        sourceKey: meta.sourceKey || undefined,
        outputBucket: meta.outputBucket || undefined,
        outputKey: meta.outputKey,
        logKey: meta.logKey || undefined,

        fileVersionId: meta.fileVersionId || undefined,
        fileName: meta.fileName || undefined,
        originalName: meta.originalName || undefined,
        mimeType: meta.mimeType || undefined,
        extension: meta.extension || undefined,

        fileType,
        size,
        userTier,
        userId: meta.userId || "retry-user",
        batchId: meta.batchId || undefined,

        callbackUrl: meta.callbackUrl || undefined,
        callbackToken:
          process.env.PROCESSING_CALLBACK_TOKEN || meta.callbackToken || "",
      });
    } catch (error) {
      logger.error(`recoverStuckJobs failed for ${jobId}`, { error });
    }
  }
}
