import { enqueueFile } from '../queue/enqueue';

/**
 * Retry a failed job manually
 */
export async function retryJob(job: any) {
  return enqueueFile({
    ...job,
    retryCount: (job.retryCount || 0) + 1,
  });
}