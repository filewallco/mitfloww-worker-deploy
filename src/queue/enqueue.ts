import { fileQueue } from './queues';
import { FileJob } from '../types';
import { connection } from './connection';

const MB = 1024 * 1024;

function classify(size: number) {
  if (size < 50 * MB) return 'small';
  if (size < 500 * MB) return 'medium';
  return 'large';
}

// base priority
function basePriority(job: FileJob) {
  const sizeType = classify(job.size);

  const tierWeight = {
    vip: 0,
    premium: 1,
    free: 2,
  };

  const sizeWeight = {
    small: 0,
    medium: 1,
    large: 2,
  };

  return tierWeight[job.userTier] * 10 + sizeWeight[sizeType];
}

// aging (prevents starvation)
function getPriority(job: FileJob) {
  const createdAt = Date.now();

  return basePriority(job) - Math.floor(createdAt / (1000 * 60)); 
}

export async function enqueueFile(job: FileJob) {
  const sizeType = classify(job.size);

  await connection.hset(`job:${job.fileId}`, {
    status: 'queued',
    stage: 'waiting',
    createdAt: Date.now(),
    size: job.size,
    userTier: job.userTier,
  });

  await connection.expire(`job:${job.fileId}`, 60 * 60 * 24);

  const counts = await fileQueue.getJobCounts();

  await connection.hset(`job:${job.fileId}`, {
    queuePosition: counts.waiting || 0,
  });

  return fileQueue.add(sizeType, job, {
    jobId: job.fileId,
    priority: getPriority(job),
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 5000,
    },
    removeOnComplete: true,
  });
}