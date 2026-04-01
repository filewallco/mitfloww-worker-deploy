import { fileQueue } from '../queue/queues';
import { connection } from '../queue/connection';

export async function getJobStatus(id: string) {
  const job = await fileQueue.getJob(id);

  const redisMeta = await connection.hgetall(`job:${id}`);

  if (!job && !redisMeta) {
    return { status: 'not_found' };
  }

  return {
    queueState: job ? await job.getState() : null,
    progress: job?.progress || null,
    meta: redisMeta,
  };
}