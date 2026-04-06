import { smallQueue, mediumQueue, largeQueue, imageQueue } from '../queue/queues';
import { connection } from '../queue/connection';

/**
 * Retrieves the status of a file processing job.
 * Combines information from BullMQ queue and Redis metadata.
 * 
 * @param id - The unique ID of the job/file
 * @returns An object containing:
 *  - queueState: current BullMQ state ('completed', 'failed', 'waiting', etc.)
 *  - progress: job progress (0-100)
 *  - meta: stored Redis metadata for the job
 *  - status: 'not_found' if job does not exist
 */
export async function getJobStatus(id: string) {
  // Attempt to fetch the job from BullMQ
  const job = await imageQueue.getJob(id) || await smallQueue.getJob(id) || await mediumQueue.getJob(id) || await largeQueue.getJob(id);

  // Retrieve job metadata from Redis
  const redisMeta = await connection.hgetall(`job:${id}`);

  // If neither queue job nor Redis metadata exists, return not_found
  if (!job && Object.keys(redisMeta).length === 0) {
    return { status: 'not_found' };
  }

  return {
    queueState: job ? await job.getState() : null,
    progress: job?.progress || null,
    meta: redisMeta,
  };
}