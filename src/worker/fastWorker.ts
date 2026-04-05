import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';

/**
 * Worker for SMALL files (fast lane)
 * Dedicated queue → no filtering needed
 */
new Worker(
  'small-files',
  async (job) => {
    console.log('FAST worker:', job.id);
    await handleJob(job.data, job);
  },
  {
    connection,
    concurrency: 5, // high concurrency for small files
  }
);