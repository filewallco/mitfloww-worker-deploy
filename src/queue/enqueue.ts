import { smallQueue, mediumQueue, largeQueue } from './queues';
import { FileJob } from '../types';

const MB = 1024 * 1024;

function classify(size: number) {
  if (size < 50 * MB) return 'small';
  if (size < 500 * MB) return 'medium';
  return 'large';
}

export async function enqueueFile(job: FileJob) {
  const type = classify(job.size);

  const options = {
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 5000,
    },
  };

  if (type === 'small') return smallQueue.add('job', job, options);
  if (type === 'medium') return mediumQueue.add('job', job, options);
  return largeQueue.add('job', job, options);
}