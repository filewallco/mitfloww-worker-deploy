import path from 'path';
import os from 'os';

export const config = {
  mode: process.env.MODE || 'local',

  redis: {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
  },

  ffmpegPath: process.env.FFMPEG_PATH || 'ffmpeg',

  tempDir:
    process.env.TEMP_DIR ||
    path.join(os.tmpdir(), 'filewall'),

  outputDir:
    process.env.OUTPUT_DIR ||
    path.resolve('./outputs'),

  concurrency: Number(process.env.MAX_CONCURRENCY) || 5,

  rateLimit: {
    max: Number(process.env.RATE_LIMIT_MAX) || 10,
    duration: Number(process.env.RATE_LIMIT_DURATION) || 1000,
  },
};