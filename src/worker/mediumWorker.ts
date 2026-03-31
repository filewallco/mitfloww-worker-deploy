import { Worker } from 'bullmq';
import { connection } from '../queue/connection';
import { handleJob } from './handler';
import { start, done } from './admission';

new Worker(
  'medium-files',
  async (job) => {
    console.log('Medium worker started');
    
    start('medium');
    try {
      await handleJob(job.data);
    } finally {
      done('medium');
    }
  },
  { connection, concurrency: 5 }
);