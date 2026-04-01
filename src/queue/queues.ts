import { Queue } from 'bullmq';
import { connection } from './connection';

/**
 * BullMQ queue for file processing jobs.
 * Uses the shared Redis connection.
 */
export const fileQueue = new Queue('file-processing', {
  connection,
});