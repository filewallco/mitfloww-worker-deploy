import { onBullJobFailed } from './workerFailure';
import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { config } from '../config';
import { logger } from '../utils/logger';

/**
 * Worker for SMALL files (fast lane)
 * Dedicated queue → no filtering needed
 */
const fastWorker = new Worker(
  'small-files',
  async (job, token) => {
    logger.info('FAST worker picked up job', { jobId: job.id, data: job.data?.fileId });
    await handleJob(job.data, job, token);
  },
  {
    connection,
    concurrency: config.fast,
  }
);

fastWorker.on('failed', (job, err) => {
  void onBullJobFailed('FAST', job, err);
});

fastWorker.on('error', (err) => {
  logger.error('FAST worker error', { error: err });
});
