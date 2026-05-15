import path from 'path';
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
 * Centralized configuration for MitFloww application.
 * Environment variables take precedence, defaults are fallback values.
 */
export const config = {
  /** Running mode: 'local' for development/testing, 'server' for production */
  mode: process.env.MODE || 'server',

  /** Redis connection configuration */
  redis: {
    host: validateEnv('REDIS_HOST', process.env.REDIS_HOST || '127.0.0.1'),
    port: Number(validateEnv('REDIS_PORT', process.env.REDIS_PORT || 6379)),
  },

  /** Path to FFmpeg binary */
  ffmpegPath: validateEnv('FFMPEG_PATH', process.env.FFMPEG_PATH || 'ffmpeg'),

  /** Path to FFprobe binary */
  ffprobePath: validateEnv('FFPROBE_PATH', process.env.FFPROBE_PATH || 'ffprobe'),

  /** Temporary directory for processing files */
  tempDir: process.env.TEMP_DIR || path.join(process.cwd(), 'tmp'),

  /** Directory where processed outputs are saved */
  outputDir: process.env.OUTPUT_DIR || path.join(process.cwd(), 'outputs'),

  /**
   * Worker concurrency tuning
   *
   * IMPORTANT:
   * These values must be aligned with CPU + disk constraints.
   *
   * Strategy:
   * - fast   → high throughput, low cost jobs
   * - medium → balanced
   * - heavy  → strictly serialized (avoid system lock)
   * - image  → lightweight CPU tasks
   */
  fast: Number(validateEnv('FAST_CONCURRENCY', process.env.FAST_CONCURRENCY || 2)),
  medium: Number(validateEnv('MEDIUM_CONCURRENCY', process.env.MEDIUM_CONCURRENCY || 2)),
  heavy: Number(validateEnv('HEAVY_CONCURRENCY', process.env.HEAVY_CONCURRENCY || 1)), // MUST stay 1
  image: Number(validateEnv('IMAGE_CONCURRENCY', process.env.IMAGE_CONCURRENCY || 4)),

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

  /** Security limits */
  security: {
    maxUploadBytes: Number(process.env.MAX_UPLOAD_BYTES || 5 * 1024 * 1024 * 1024), // 5GB
    maxOutputBytes: Number(process.env.MAX_OUTPUT_BYTES || 5 * 1024 * 1024 * 1024), // 5GB
    maxImagePixels: Number(process.env.MAX_IMAGE_PIXELS || 50_000_000),
    maxPdfBytes: Number(process.env.MAX_PDF_BYTES || 100 * 1024 * 1024), // 100MB
    maxPdfPages: Number(process.env.MAX_PDF_PAGES || 100),
    maxPdfPageDimension: Number(process.env.MAX_PDF_PAGE_DIMENSION || 14_400),
    pdfProcessingTimeoutMs: Number(process.env.PDF_PROCESSING_TIMEOUT_MS || 60_000),
  },

  /** Privileged endpoint tokens */
  adminToken: process.env.ADMIN_TOKEN || '',
  wsToken: process.env.WS_TOKEN || process.env.ADMIN_TOKEN || '',

  /** User tier limits for concurrent processing */
  userLimits: {
    free: Number(process.env.USER_LIMIT_FREE || 2),
    premium: Number(process.env.USER_LIMIT_PREMIUM || 4),
    vip: Number(process.env.USER_LIMIT_VIP || 6),
  },
};

// Ensure temp/output directories exist
if (!fs.existsSync(config.tempDir)) fs.mkdirSync(config.tempDir, { recursive: true });
if (!fs.existsSync(config.outputDir)) fs.mkdirSync(config.outputDir, { recursive: true });

console.log('Configuration loaded:', {
  mode: config.mode,
  redis: config.redis,
  ffmpegPath: config.ffmpegPath,
  ffprobePath: config.ffprobePath,
  tempDir: config.tempDir,
  outputDir: config.outputDir,
  rateLimit: config.rateLimit,
});
