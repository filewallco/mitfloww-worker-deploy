import IORedis from 'ioredis';
import { config } from '../config';

export const connection = new IORedis({
  host: config.redis.host,
  port: config.redis.port,
  maxRetriesPerRequest: null,
});