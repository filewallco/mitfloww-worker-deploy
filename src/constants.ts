// Job lifecycle states
export const JOB_STATUS = {
  QUEUED: 'queued',
  PROCESSING: 'processing',
  PROCESSED: 'processed',
  UPLOADING: 'uploading',
  COMPLETED: 'completed',
  RETRYING: 'retrying',
  FAILED: 'failed',
} as const;

export type JobStatus = typeof JOB_STATUS[keyof typeof JOB_STATUS];

// Stages (UI / logs)
export const JOB_STAGE = {
  WAITING: 'waiting',
  STARTING: 'starting',
  DOWNLOADING: 'downloading',
  PROCESSING: 'processing',
  UPLOADING: 'uploading',
  DONE: 'done',
  FAILED: 'failed',
  CANCELLED: 'cancelled',
  STUCK_RECOVERY: 'stuck_recovery',
} as const;

// Queue names
export const QUEUE_NAME = {
  SMALL: 'small-files',
  MEDIUM: 'medium-files',
  LARGE: 'large-files',
  IMAGE: 'image-files',
} as const;

// File types
export const FILE_TYPE = {
  VIDEO: 'video',
  IMAGE: 'image',
  PDF: 'pdf',
  ZIP: 'zip',
  OTHER: 'other',
} as const;

// Redis keys
export const REDIS_KEYS = {
  JOB: (id: string) => `job:${id}`,
  JOB_LOGS: (id: string) => `job:${id}:logs`,
  PREVIEW: (id: string) => `preview:${id}`,
  LOCK: (id: string) => `lock:${id}`,
  DLQ: 'dead-letter-queue',
};

// MB size
export const MB = 1024 * 1024;