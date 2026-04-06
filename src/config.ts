import path from 'path';
import os from 'os';
import fs from 'fs';

/**
 * Validate required environment variable or fallback
 * @param name Variable name
 * @param value Value to validate
 */
function validateEnv(name: string, value: any) {
  if (value === undefined || value === '') {
    throw new Error(`Missing or invalid environment variable: ${name}`);
  }
  return value;
}

/**
 * Centralized configuration for FileWall application.
 * Environment variables take precedence, defaults are fallback values.
 */
export const config = {
  /** Running mode: 'local' for development/testing, 'server' for production */
  mode: validateEnv('MODE', process.env.MODE || 'local'),

  /** Redis connection configuration */
  redis: {
    host: validateEnv('REDIS_HOST', process.env.REDIS_HOST || '127.0.0.1'),
    port: Number(validateEnv('REDIS_PORT', process.env.REDIS_PORT || 6379)),
  },

  /** Path to FFmpeg binary */
  ffmpegPath: validateEnv('FFMPEG_PATH', process.env.FFMPEG_PATH || 'ffmpeg'),

  /** Temporary directory for processing files */
  tempDir: process.env.TEMP_DIR || path.join(os.tmpdir(), 'filewall'),

  /** Directory where processed outputs are saved */
  outputDir: process.env.OUTPUT_DIR || path.resolve('./outputs'),

  /** Maximum concurrency for job processing */
  concurrency: Number(validateEnv('MAX_CONCURRENCY', process.env.MAX_CONCURRENCY || 5)),

  /** Rate limit configuration (e.g., for uploads or API requests) */
  rateLimit: {
    max: Number(validateEnv('RATE_LIMIT_MAX', process.env.RATE_LIMIT_MAX || 10)),
    duration: Number(validateEnv('RATE_LIMIT_DURATION', process.env.RATE_LIMIT_DURATION || 1000)), // in ms
  },

  /** Disk safety thresholds */
  disk: {
    minFreeBytes: Number(process.env.MIN_FREE_DISK || 5 * 1024 * 1024 * 1024), // 5GB
    targetFreeBytes: Number(process.env.TARGET_FREE_DISK || 10 * 1024 * 1024 * 1024), // 10GB
  },
};

// Ensure temp/output directories exist
if (!fs.existsSync(config.tempDir)) fs.mkdirSync(config.tempDir, { recursive: true });
if (!fs.existsSync(config.outputDir)) fs.mkdirSync(config.outputDir, { recursive: true });

console.log('Configuration loaded:', {
  mode: config.mode,
  redis: config.redis,
  ffmpegPath: config.ffmpegPath,
  tempDir: config.tempDir,
  outputDir: config.outputDir,
  concurrency: config.concurrency,
  rateLimit: config.rateLimit,
});