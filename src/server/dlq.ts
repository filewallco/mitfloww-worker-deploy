import { REDIS_KEYS } from '../constants';
import { connection } from '../queue/connection';

/**
 * Returns dead-letter queue jobs
 */
export async function getDLQ() {
  const jobs = await connection.lrange(REDIS_KEYS.DLQ, 0, -1);
  return jobs.map((j) => JSON.parse(j));
}