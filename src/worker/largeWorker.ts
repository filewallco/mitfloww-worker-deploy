import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { start, done } from './admission';

new Worker(
  'large-files',
  async (job) => {
    console.log('Large worker started');

    start('large');
    try {
      await handleJob(job.data);
    } finally {
      done('large');
    }
  },
  { connection, concurrency: 3 }
);