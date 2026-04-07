import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { config } from '../config';

/**
 * Worker for MEDIUM files
 */
new Worker(
  'medium-files',
  async (job) => {
    console.log('STANDARD worker:', job.id);
    await handleJob(job.data, job);
  },
  {
    connection,
    concurrency: config.medium,
  }
);