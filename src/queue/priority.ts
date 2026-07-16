import { FILE_TYPE, MB, REDIS_KEYS } from "../constants";
import { FileJob } from "../types";
import { connection } from "./connection";

/**
 * Classifies file size into categories
 * @param size - Size of the file in bytes
 * @returns 'small' | 'medium' | 'large'
 */
export function classify(size: number): 'small' | 'medium' | 'large' {
  if (size < 100 * MB) return 'small';
  if (size < 800 * MB) return 'medium';
  return 'large';
}

/**
 * Computes base priority for a job based on:
 * 1. User tier: Business < Studio < Pro < Standard < Free
 * 2. File size: Small < Medium < Large
 * Lower values indicate higher priority
 *
 * @param job - File job object
 * @returns numeric base priority
 */
export function basePriority(job: FileJob): number {
  if (job.fileType === FILE_TYPE.IMAGE) return -10;
  const sizeType = classify(job.size);

  const tierWeight: Record<string, number> = {
      business: 0,
      studio: 1,
      pro: 2,
      standard: 3,
      free: 4,
  };

  const sizeWeight = {
    small: 0,
    medium: 1,
    large: 2,
  };

  const tier = tierWeight[job.userTier] ?? tierWeight.free;

  return tier * 10 + sizeWeight[sizeType];
}

/**
 * Computes final priority including aging to prevent starvation.
 * Uses bounded aging based on waiting time.
 *
 * IMPORTANT:
 * - Lower number = higher priority in BullMQ
 * - Aging reduces priority value over time (boosts older jobs)
 */
export async function getPriority(job: FileJob): Promise<number> {
  const base = basePriority(job);

  /**
   * Use a small aging factor to boost older jobs over time.
   * This prevents starvation of lower-priority jobs.
   * Example:
   * - Every minute reduces priority slightly
   */
  const agingFactor = 0.1; // tuneable
  const createdAtRaw = await connection.hget(REDIS_KEYS.JOB(job.fileId), 'createdAt');
  const createdAt = createdAtRaw ? Number(createdAtRaw) : Date.now();

  const waitingMinutes = (Date.now() - createdAt) / 60000;

  return Math.floor(base - waitingMinutes * agingFactor);
}