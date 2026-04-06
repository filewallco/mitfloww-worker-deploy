import { Queue } from 'bullmq';
import { connection } from './connection';

/**
 * BullMQ queue for file processing jobs.
 * Uses the shared Redis connection.
 */
export const smallQueue = new Queue('small-files', { connection });
export const mediumQueue = new Queue('medium-files', { connection });
export const largeQueue = new Queue('large-files', { connection });
export const imageQueue = new Queue('image-files', { connection });