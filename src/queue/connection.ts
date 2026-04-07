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

  retryStrategy(times) {
    const delay = Math.min(times * 100, 3000);
    console.warn(`Redis retry attempt ${times}, delay ${delay}ms`);
    return delay;
  },

  reconnectOnError(err) {
    console.error('Redis reconnect on error:', err.message);
    return true;
  }
});

connection.on('error', (err) => {
  console.error('Redis error:', err);
});

connection.on('connect', () => {
  console.log('Redis connected');
});