import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';

new Worker(
  'file-processing',
  async (job) => {
    console.log('STANDARD worker:', job.id);
    await handleJob(job.data, job);
  },
  {
    connection,
    concurrency: 3,
  }
);