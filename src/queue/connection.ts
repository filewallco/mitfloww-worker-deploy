import IORedis from 'ioredis';
import { config } from '../config';

/**
 * Redis connection instance using ioredis.
 * Configured based on the application's Redis settings.
 *
 * maxRetriesPerRequest: null disables automatic retries to prevent blocking on failed commands.
 */
export const connection = new IORedis({
  host: config.redis.host,
  port: config.redis.port,
  maxRetriesPerRequest: null,
});