import { Job } from 'bullmq';
import { connection } from '../queue/connection';
import { FileJob } from '../types';
import { JOB_STAGE, JOB_STATUS, REDIS_KEYS } from '../constants';
import { logger } from '../utils/logger';
import { notifyCallback } from './handler';
import { releaseDisk } from './resourceManager';

export async function onBullJobFailed(queueName: string, job: Job<FileJob> | undefined, err: Error) {
  logger.error(`${queueName} worker job failed`, { jobId: job?.id, error: err });
  if (!job?.id) return;

  const jobId = job.id;
  try {
    const meta = await connection.hgetall(REDIS_KEYS.JOB(jobId));
    if (!meta || Object.keys(meta).length === 0) return;

    if (meta.status === JOB_STATUS.COMPLETED || meta.status === JOB_STATUS.FAILED) {
      return;
    }

    const attemptsMade = job.attemptsMade ?? 0;
    const max = Number(job.opts?.attempts ?? 3);
    const isTerminal = attemptsMade >= max || err.message?.includes('maxStalledCount') || err.name === 'UnrecoverableError';

    if (!isTerminal) {
      return;
    }

    const now = Date.now();
    await connection.hset(REDIS_KEYS.JOB(jobId), {
      status: JOB_STATUS.FAILED,
      stage: JOB_STAGE.FAILED,
      error: err.message || 'Worker processing failed',
      errorCode: 'worker_job_failed',
      failedAt: now,
      updatedAt: now,
    });

    await releaseDisk(jobId).catch(() => {});
    await connection.del(REDIS_KEYS.LOCK(jobId)).catch(() => {});

    if (meta.fileVersionId) {
      await connection.del(REDIS_KEYS.ACTIVE_FILE_VERSION(meta.fileVersionId)).catch(() => {});
      await connection.del(REDIS_KEYS.QUEUED_FILE_VERSION(meta.fileVersionId)).catch(() => {});
    }

    if (meta.callbackUrl) {
      const callbackPayload = {
        status: 'failed',
        errorCode: 'worker_job_failed',
        errorMessage: err.message || 'Worker processing failed',
      };
      await connection.set(REDIS_KEYS.PENDING_CALLBACK(jobId), JSON.stringify(callbackPayload));
      await notifyCallback(
        {
          fileId: jobId,
          fileVersionId: meta.fileVersionId,
          callbackUrl: meta.callbackUrl,
          callbackToken: meta.callbackToken,
        },
        callbackPayload,
      );
    }
  } catch (failureError) {
    logger.error('Failed to handle worker job failure', { jobId, error: failureError });
  }
}
