import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';

/**
 * Worker for fast processing (small files)
 */
new Worker(
  'file-processing',
  async (job) => {
    if (job.name !== 'small') return;
    console.log('FAST worker:', job.id);
    await handleJob(job.data, job);
  },
  { connection, concurrency: 2 }
);