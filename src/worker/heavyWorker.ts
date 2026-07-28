import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { config } from '../config';
import { logger } from '../utils/logger';

/**
 * Worker for LARGE files (heavy processing)
 */
const heavyWorker = new Worker(
  'large-files',
  async (job, token) => {
    logger.info('HEAVY worker picked up job', { jobId: job.id, data: job.data?.fileId });
    await handleJob(job.data, job, token);
  },
  {
    connection,
    concurrency: config.heavy,
  }
);

heavyWorker.on('failed', (job, err) => {
  logger.error('HEAVY worker job failed', { jobId: job?.id, error: err });
});

heavyWorker.on('error', (err) => {
  logger.error('HEAVY worker error', { error: err });
});
