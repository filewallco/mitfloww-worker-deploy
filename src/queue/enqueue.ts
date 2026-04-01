import { fileQueue } from './queues';
import { FileJob } from '../types';
import { connection } from './connection';

const MB = 1024 * 1024;

/**
 * Classifies file size into categories
 * @param size - Size of the file in bytes
 * @returns 'small' | 'medium' | 'large'
 */
function classify(size: number): 'small' | 'medium' | 'large' {
  if (size < 50 * MB) return 'small';
  if (size < 500 * MB) return 'medium';
  return 'large';
}

/**
 * Computes base priority for a job based on:
 * 1. User tier: VIP < Premium < Free
 * 2. File size: Small < Medium < Large
 * Lower values indicate higher priority
 *
 * @param job - File job object
 * @returns numeric base priority
 */
function basePriority(job: FileJob): number {
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

/**
 * Computes final priority including aging to prevent starvation.
 * Older jobs gradually gain higher priority.
 *
 * @param job - File job object
 * @returns numeric priority to pass to BullMQ
 */
function getPriority(job: FileJob): number {
  const createdAt = Date.now();

  // Subtracting minutes from base priority to age the job
  return basePriority(job) - Math.floor(createdAt / (1000 * 60));
}

/**
 * Enqueues a file processing job:
 * - Stores job metadata in Redis (status, stage, size, user tier)
 * - Sets a TTL for the job metadata
 * - Calculates queue position
 * - Adds job to BullMQ queue with priority, retry, and backoff settings
 *
 * @param job - File job to enqueue
 * @returns Promise resolving to the BullMQ Job object
 */
export async function enqueueFile(job: FileJob) {
  const sizeType = classify(job.size);

  // Store job metadata in Redis
  await connection.hset(`job:${job.fileId}`, {
    status: 'queued',
    stage: 'waiting',
    createdAt: Date.now(),
    size: job.size,
    userTier: job.userTier,
  });

  // Set a TTL of 24 hours for job metadata
  await connection.expire(`job:${job.fileId}`, 60 * 60 * 24);

  // Get current counts to calculate queue position
  const counts = await fileQueue.getJobCounts();

  await connection.hset(`job:${job.fileId}`, {
    queuePosition: counts.waiting || 0,
  });

  // Add job to BullMQ queue with priority and retry/backoff rules
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