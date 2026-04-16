import { enqueueFile } from '../queue/enqueue';

/**
 * Retry a failed job manually
 */
export async function retryJob(job: any) {
  return enqueueFile({
    ...job,
    userId: job.userId || 'retry-user',
    batchId: job.batchId || undefined,
    retryCount: (job.retryCount || 0) + 1,
  });
}