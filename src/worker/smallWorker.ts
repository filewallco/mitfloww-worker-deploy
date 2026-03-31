import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { start, done } from './admission';

new Worker(
  'small-files',
  async (job) => {
    start('small');
    try {
      await handleJob(job.data);
    } finally {
      done('small');
    }
  },
  { connection, concurrency: 2 }
);