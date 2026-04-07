import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { config } from '../config';

/**
 * Worker for LARGE files (heavy processing)
 */
new Worker(
  'large-files',
  async (job) => {
    console.log('HEAVY worker:', job.id);
    await handleJob(job.data, job);
  },
  {
    connection,
    concurrency: config.concurrency.heavy, // heavy jobs → low concurrency
  }
);