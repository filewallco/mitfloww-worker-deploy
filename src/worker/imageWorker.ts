import { onBullJobFailed } from './workerFailure';
import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { config } from '../config';
import { logger } from '../utils/logger';

const imageWorker = new Worker(
  'image-files',
  async (job, token) => {
    logger.info('IMAGE worker picked up job', { jobId: job.id, data: job.data?.fileId });
    await handleJob(job.data, job, token);
  },
  {
    connection,
    concurrency: config.image,
  }
);

imageWorker.on('failed', (job, err) => {
  void onBullJobFailed('IMAGE', job, err);
});

imageWorker.on('error', (err) => {
  logger.error('IMAGE worker error', { error: err });
});
