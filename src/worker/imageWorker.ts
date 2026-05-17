import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { config } from '../config';

new Worker(
  'image-files',
  async (job, token) => {
    console.log('IMAGE worker:', job.id);
    await handleJob(job.data, job, token);
  },
  {
    connection,
    concurrency: config.image,
  }
);
