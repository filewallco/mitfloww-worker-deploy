import { Job } from 'bullmq';
import { FILE_TYPE, JOB_STATUS, QUEUE_NAME, REDIS_KEYS } from '../constants';
import { connection } from '../queue/connection';
import { imageQueue, largeQueue, mediumQueue, smallQueue } from '../queue/queues';
import { estimateProcessingMs } from '../utils/eta';

export type PublicJobStatus = {
  jobId: string;
  fileVersionId: string | null;
  status: string;
  stage: string | null;
  progress: number;
  queueName: string | null;
  queuePosition: number | null;
  queueSize: number | null;
  queuedAt: number | null;
  startedAt: number | null;
  updatedAt: number | null;
  heartbeatAt: number | null;
  waitReason: string | null;
  nextRetryAt: number | null;
  attemptsMade: number;
  maxAttempts: number;
  resourceWaitCount: number;
  estimatedStartAt: number | null;
  estimatedCompletionAt: number | null;
  etaMs: number | null;
  queueEtaMs: number | null;
  processingEtaMs: number | null;
  error: { code?: string; message: string } | null;
};

function toNumber(value: unknown): number | null {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function pickQueue(queueName: string | null | undefined) {
  if (queueName === QUEUE_NAME.IMAGE) return imageQueue;
  if (queueName === QUEUE_NAME.SMALL) return smallQueue;
  if (queueName === QUEUE_NAME.MEDIUM) return mediumQueue;
  if (queueName === QUEUE_NAME.LARGE) return largeQueue;
  return null;
}

async function findJobAcrossQueues(jobId: string): Promise<{ queueName: string | null; job: Job | null }> {
  const fromImage = await imageQueue.getJob(jobId);
  if (fromImage) return { queueName: QUEUE_NAME.IMAGE, job: fromImage };

  const fromSmall = await smallQueue.getJob(jobId);
  if (fromSmall) return { queueName: QUEUE_NAME.SMALL, job: fromSmall };

  const fromMedium = await mediumQueue.getJob(jobId);
  if (fromMedium) return { queueName: QUEUE_NAME.MEDIUM, job: fromMedium };

  const fromLarge = await largeQueue.getJob(jobId);
  if (fromLarge) return { queueName: QUEUE_NAME.LARGE, job: fromLarge };

  return { queueName: null, job: null };
}

async function estimateJobDuration(job: Job): Promise<number> {
  const fileType = String(job.data?.fileType || FILE_TYPE.OTHER);
  const size = Number(job.data?.size || 0);
  return await estimateProcessingMs(fileType, size);
}

async function getQueuePositionAndEta(
  queueName: string | null,
  bullJob: Job | null,
  now: number,
): Promise<{ queuePosition: number | null; queueSize: number | null; queueEtaMs: number | null }> {
  if (!queueName) {
    return { queuePosition: null, queueSize: null, queueEtaMs: null };
  }

  const queue = pickQueue(queueName);
  if (!queue) {
    return { queuePosition: null, queueSize: null, queueEtaMs: null };
  }

  const [waitingCount, delayedCount, activeCount] = await Promise.all([
    queue.getWaitingCount(),
    queue.getDelayedCount(),
    queue.getActiveCount(),
  ]);
  const queueSize = waitingCount + delayedCount + activeCount;

  if (!bullJob) {
    return { queuePosition: null, queueSize, queueEtaMs: null };
  }

  const state = await bullJob.getState();
  if (state === 'active') {
    return { queuePosition: 0, queueSize, queueEtaMs: 0 };
  }

  if (state === 'waiting' || state === 'prioritized') {
    const waitingJobs = await queue.getWaiting();
    const index = waitingJobs.findIndex((candidate) => candidate.id === bullJob.id);
    if (index < 0) {
      return { queuePosition: null, queueSize, queueEtaMs: null };
    }

    const aheadJobs = waitingJobs.slice(0, index);
    const estimated = await Promise.all(aheadJobs.map((candidate) => estimateJobDuration(candidate)));
    const queueEtaMs = estimated.reduce((sum, value) => sum + value, 0);
    return {
      queuePosition: index + 1,
      queueSize,
      queueEtaMs,
    };
  }

  if (state === 'delayed') {
    const delayedJobs = await queue.getDelayed();
    const index = delayedJobs.findIndex((candidate) => candidate.id === bullJob.id);
    const delayedUntil = Number(bullJob.timestamp || now) + Number(bullJob.delay || 0);
    const baseWait = Math.max(0, delayedUntil - now);
    return {
      queuePosition: index >= 0 ? index + 1 : null,
      queueSize,
      queueEtaMs: baseWait,
    };
  }

  return {
    queuePosition: null,
    queueSize,
    queueEtaMs: null,
  };
}

function computeProcessingEta(
  status: string,
  progress: number,
  startedAt: number | null,
  estimatedMs: number,
  now: number,
): number | null {
  if (status === JOB_STATUS.COMPLETED || status === JOB_STATUS.FAILED || status === JOB_STATUS.CANCELLED) {
    return null;
  }

  if (!startedAt) return estimatedMs;
  const elapsed = Math.max(0, now - startedAt);

  if (progress > 0 && progress < 100) {
    const eta = Math.round((elapsed * (100 - progress)) / progress);
    return eta >= 0 ? eta : null;
  }

  const fallback = Math.max(0, Math.round(estimatedMs - elapsed));
  return fallback;
}

export async function getPublicJobStatus(id: string): Promise<PublicJobStatus | null> {
  const [meta, queueLookup] = await Promise.all([
    connection.hgetall(REDIS_KEYS.JOB(id)),
    findJobAcrossQueues(id),
  ]);

  if (!queueLookup.job && Object.keys(meta).length === 0) {
    return null;
  }

  const now = Date.now();
  const queueName = meta.queueName || queueLookup.queueName;
  const status = meta.status || (queueLookup.job ? await queueLookup.job.getState() : 'queued');
  const stage = meta.stage || null;
  const progressFromMeta = toNumber(meta.progress);
  const progressFromBull =
    typeof queueLookup.job?.progress === 'number' ? Number(queueLookup.job.progress) : null;
  const progress = Math.max(0, Math.min(100, progressFromMeta ?? progressFromBull ?? 0));
  const queuedAt = toNumber(meta.queuedAt);
  const startedAt = toNumber(meta.startedAt);
  const updatedAt = toNumber(meta.updatedAt);
  const heartbeatAt = toNumber(meta.heartbeatAt);
  const nextRetryAt = toNumber(meta.nextRetryAt);
  const attemptsMade = Number(
    queueLookup.job?.attemptsMade ?? toNumber(meta.attemptsMade) ?? 0,
  );
  const maxAttempts = Number(
    queueLookup.job?.opts?.attempts ?? toNumber(meta.maxAttempts) ?? 1,
  );
  const resourceWaitCount = Number(toNumber(meta.resourceWaitCount) ?? 0);
  const waitReason = meta.waitReason || null;
  const fileType = meta.fileType || String(queueLookup.job?.data?.fileType || FILE_TYPE.OTHER);
  const size = Number(toNumber(meta.size) ?? Number(queueLookup.job?.data?.size || 0));
  const estimatedMs = await estimateProcessingMs(fileType, size);

  const queueSnapshot = await getQueuePositionAndEta(queueName, queueLookup.job, now);
  const queueEtaMs = queueSnapshot.queueEtaMs;
  const processingEtaMs = computeProcessingEta(status, progress, startedAt, estimatedMs, now);

  const etaMs =
    queueEtaMs === null && processingEtaMs === null
      ? null
      : (queueEtaMs || 0) + (processingEtaMs || 0);

  const estimatedStartAt =
    status === JOB_STATUS.PROCESSING || status === JOB_STATUS.UPLOADING
      ? startedAt || now
      : queueEtaMs !== null
        ? now + queueEtaMs
        : nextRetryAt;

  const estimatedCompletionAt =
    etaMs !== null
      ? now + etaMs
      : null;

  return {
    jobId: id,
    fileVersionId: meta.fileVersionId || null,
    status,
    stage,
    progress,
    queueName: queueName || null,
    queuePosition: queueSnapshot.queuePosition,
    queueSize: queueSnapshot.queueSize,
    queuedAt,
    startedAt,
    updatedAt,
    heartbeatAt,
    waitReason,
    nextRetryAt,
    attemptsMade,
    maxAttempts,
    resourceWaitCount,
    estimatedStartAt,
    estimatedCompletionAt,
    etaMs,
    queueEtaMs,
    processingEtaMs,
    error: meta.error ? { code: meta.errorCode || undefined, message: meta.error } : null,
  };
}

export async function getPublicJobStatuses(ids: string[]): Promise<PublicJobStatus[]> {
  const statuses = await Promise.all(ids.map((id) => getPublicJobStatus(id)));
  return statuses.map((status, index) => {
    if (status) return status;
    return {
      jobId: ids[index],
      fileVersionId: null,
      status: 'not_found',
      stage: null,
      progress: 0,
      queueName: null,
      queuePosition: null,
      queueSize: null,
      queuedAt: null,
      startedAt: null,
      updatedAt: null,
      heartbeatAt: null,
      waitReason: null,
      nextRetryAt: null,
      attemptsMade: 0,
      maxAttempts: 0,
      resourceWaitCount: 0,
      estimatedStartAt: null,
      estimatedCompletionAt: null,
      etaMs: null,
      queueEtaMs: null,
      processingEtaMs: null,
      error: null,
    };
  });
}
