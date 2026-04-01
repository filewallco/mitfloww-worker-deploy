import { Queue } from 'bullmq';
import { connection } from './connection';

export const fileQueue = new Queue('file-processing', {
  connection,
});