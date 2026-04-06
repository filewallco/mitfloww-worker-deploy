import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';

new Worker(
  'image-files',
  async (job) => {
    console.log('IMAGE worker:', job.id);
    await handleJob(job.data, job);
  },
  {
    connection,
    concurrency: 15,
  }
);