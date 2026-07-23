import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { config } from '../config';
import { logger } from '../utils/logger';

/**
 * Worker for MEDIUM files
 */
const standardWorker = new Worker(
  'medium-files',
  async (job, token) => {
    logger.info('STANDARD worker picked up job', { jobId: job.id, data: job.data?.fileId });
    await handleJob(job.data, job, token);
  },
  {
    connection,
    concurrency: config.medium,
  }
);

standardWorker.on('failed', (job, err) => {
  logger.error('STANDARD worker job failed', { jobId: job?.id, error: err });
});

standardWorker.on('error', (err) => {
  logger.error('STANDARD worker error', { error: err });
});
