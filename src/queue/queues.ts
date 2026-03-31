import { Queue } from 'bullmq';
import { connection } from './connection';

export const smallQueue = new Queue('small-files', {
  connection,
});

export const mediumQueue = new Queue('medium-files', {
  connection,
});

export const largeQueue = new Queue('large-files', {
  connection,
});