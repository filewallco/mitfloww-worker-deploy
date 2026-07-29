// Job lifecycle states
export const JOB_STATUS = {
  QUEUED: 'queued',
  PROCESSING: 'processing',
  PROCESSED: 'processed',
  UPLOADING: 'uploading',
  COMPLETED: 'completed',
  RETRYING: 'retrying',
  FAILED: 'failed',
  CANCELLED: 'cancelled',
} as const;

export type JobStatus = typeof JOB_STATUS[keyof typeof JOB_STATUS];

// Stages (UI / logs)
export const JOB_STAGE = {
  WAITING: 'waiting',
  WAITING_FOR_DISK: 'waiting_for_disk',
  WAITING_FOR_CPU: 'waiting_for_cpu',
  WAITING_FOR_USER_SLOT: 'waiting_for_user_slot',
  STARTING: 'starting',
  RESERVED: 'reserved',
  VALIDATING: 'validating',
  DELAYED: 'delayed',
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
  ACTIVE_FILE_VERSION: (fileVersionId: string) => `active:fileVersion:${fileVersionId}`,
  QUEUED_FILE_VERSION: (fileVersionId: string) => `queued:fileVersion:${fileVersionId}`,
  RESOURCE_DISK_RESERVED_TOTAL: 'resource:disk:reserved_total',
  RESOURCE_DISK_JOB: (jobId: string) => `resource:disk:job:${jobId}`,
  RESOURCE_CPU_HOLDERS: 'resource:cpu:holders', // legacy

  RESOURCE_CPU_IMAGE_HOLDERS: 'resource:cpu:image:holders',
  RESOURCE_CPU_SMALL_HOLDERS: 'resource:cpu:small:holders',
  RESOURCE_CPU_MEDIUM_HOLDERS: 'resource:cpu:medium:holders',
  RESOURCE_CPU_HEAVY_HOLDERS: 'resource:cpu:heavy:holders',
  RESOURCE_USER_HOLDERS: (userId: string) => `resource:user:${userId}:holders`,
  RESOURCE_UPLOAD_HOLDERS: 'resource:upload:holders',
  METRIC_DURATION: (fileType: string, sizeBucket: string) => `metrics:duration:${fileType}:${sizeBucket}`,
  DLQ: 'dead-letter-queue',

  // for storing the job completion status to handle failed api call
  PENDING_CALLBACK: (jobId: string) =>
    `pending_callback:${jobId}`,
};

// MB size
export const MB = 1024 * 1024;
