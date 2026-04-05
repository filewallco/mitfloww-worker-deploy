import { connection } from '../queue/connection';

/**
 * Returns dead-letter queue jobs
 */
export async function getDLQ() {
  const jobs = await connection.lrange('dead-letter-queue', 0, -1);
  return jobs.map((j) => JSON.parse(j));
}