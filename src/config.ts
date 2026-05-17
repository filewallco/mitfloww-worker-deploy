import fs from 'fs';
import path from 'path';

function validateEnv(name: string, value: unknown) {
  if (value === undefined || value === null || value === '') {
    throw new Error(`Missing or invalid environment variable: ${name}`);
  }
  return value;
}

function num(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid numeric environment variable: ${name}`);
  }
  return parsed;
}

export const config = {
  mode: process.env.MODE || 'server',

  redis: {
    host: String(validateEnv('REDIS_HOST', process.env.REDIS_HOST || '127.0.0.1')),
    port: num('REDIS_PORT', 6379),
  },

  ffmpegPath: String(validateEnv('FFMPEG_PATH', process.env.FFMPEG_PATH || 'ffmpeg')),
  ffprobePath: String(validateEnv('FFPROBE_PATH', process.env.FFPROBE_PATH || 'ffprobe')),

  tempDir: process.env.TEMP_DIR || path.join(process.cwd(), 'tmp'),
  outputDir: process.env.OUTPUT_DIR || path.join(process.cwd(), 'outputs'),

  fast: num('FAST_CONCURRENCY', 2),
  medium: num('MEDIUM_CONCURRENCY', 2),
  heavy: num('HEAVY_CONCURRENCY', 1),
  image: num('IMAGE_CONCURRENCY', 4),

  rateLimit: {
    max: num('RATE_LIMIT_MAX', 10),
    duration: num('RATE_LIMIT_DURATION', 1000),
  },

  disk: {
    minFreeBytes: num('MIN_FREE_DISK', 5 * 1024 * 1024 * 1024),
    targetFreeBytes: num('TARGET_FREE_DISK', 10 * 1024 * 1024 * 1024),
    reservationMultiplier: num('DISK_RESERVATION_MULTIPLIER', 2.5),
    reservationMinBytes: num('DISK_RESERVATION_MIN_BYTES', 524288000),
  },

  processing: {
    maxAttempts: num('MAX_PROCESSING_ATTEMPTS', 3),
    maxManualRetries: num('MAX_MANUAL_RETRIES', 3),
    maxResourceWaitMs: num('MAX_RESOURCE_WAIT_MS', 86400000),
    maxResourceWaitCount: num('MAX_RESOURCE_WAIT_COUNT', 1440),
    resourceRetryDelayMs: num('RESOURCE_RETRY_DELAY_MS', 60000),
  },

  cleanup: {
    tempCleanupMinAgeMs: num('TEMP_CLEANUP_MIN_AGE_MS', 3600000),
    failedTempRetentionMs: num('FAILED_TEMP_RETENTION_MS', 1800000),
  },

  wsSnapshotIntervalMs: num('WS_SNAPSHOT_INTERVAL_MS', 5000),

  security: {
    maxUploadBytes: num('MAX_UPLOAD_BYTES', 5 * 1024 * 1024 * 1024),
    maxOutputBytes: num('MAX_OUTPUT_BYTES', 5 * 1024 * 1024 * 1024),
    maxImagePixels: num('MAX_IMAGE_PIXELS', 50_000_000),
    maxPdfBytes: num('MAX_PDF_BYTES', 100 * 1024 * 1024),
    maxPdfPages: num('MAX_PDF_PAGES', 100),
    maxPdfPageDimension: num('MAX_PDF_PAGE_DIMENSION', 14_400),
    pdfProcessingTimeoutMs: num('PDF_PROCESSING_TIMEOUT_MS', 60_000),
  },

  resource: {
    globalCpuLimit: num('GLOBAL_CPU_LIMIT', 0), // legacy fallback only

    imageCpuLimit: num('IMAGE_CPU_LIMIT', 1),
    smallCpuLimit: num('SMALL_CPU_LIMIT', 1),
    mediumCpuLimit: num('MEDIUM_CPU_LIMIT', 1),
    heavyCpuLimit: num('HEAVY_CPU_LIMIT', 1),

    maxParallelUploads: num('MAX_PARALLEL_UPLOADS', 6),
  },

  eta: {
    videoLargeEstimateMsPerGb: num('VIDEO_LARGE_ESTIMATE_MS_PER_GB', 600000),
  },

  adminToken: process.env.ADMIN_TOKEN || '',
  wsToken: process.env.WS_TOKEN || process.env.ADMIN_TOKEN || '',

  userLimits: {
    free: num('USER_LIMIT_FREE', 2),
    premium: num('USER_LIMIT_PREMIUM', 4),
    vip: num('USER_LIMIT_VIP', 6),
  },
};

if (!fs.existsSync(config.tempDir)) fs.mkdirSync(config.tempDir, { recursive: true });
if (!fs.existsSync(config.outputDir)) fs.mkdirSync(config.outputDir, { recursive: true });
