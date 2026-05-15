import IORedis from 'ioredis';
import { config } from '../config';
import { logger } from '../utils/logger';

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
    logger.warn('Redis retry attempt', { attempt: times, delay });
    return delay;
  },

  reconnectOnError(err) {
    logger.error('Redis reconnect on error', { message: err.message });
    return true;
  }
});

connection.on('error', (err) => {
  logger.error('Redis error', { error: err });
});

connection.on('connect', () => {
  logger.info('Redis connected');
});