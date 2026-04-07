import { REDIS_KEYS } from '../constants';
import { connection } from '../queue/connection';

/**
 * Returns full job detail including logs timeline
 */
export async function getJobDetail(id: string) {
  const meta = await connection.hgetall(`job:${id}`);

  const logsRaw = await connection.lrange(REDIS_KEYS.JOB_LOGS(id), 0, -1)

  const logs = logsRaw.map((l) => JSON.parse(l));

  return {
    meta,
    logs,
  };
}