import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { config } from '../config';

new Worker(
  'image-files',
  async (job) => {
    console.log('IMAGE worker:', job.id);
    await handleJob(job.data, job);
  },
  {
    connection,
    concurrency: config.image,
  }
);