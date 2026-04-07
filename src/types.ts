/**
 * Defines the structure of a file processing job in MitFloww
 */
export interface FileJob {
  /** Unique identifier for the job/file */
  fileId: string;

  /** Source URL or file path */
  inputUrl: string;

  /** Output key / destination for processed file */
  outputKey: string;

  /** Type of the file */
  fileType: 'video' |'image' | 'pdf' | 'zip' | 'other';

  /** Size of the file in bytes */
  size: number;

  /** Tier of the user uploading the file */
  userTier: 'free' | 'premium' | 'vip';

  /** Total retry attempts across requeues */
  retryCount?: number;
}

/**
 * Maximum number of retry attempts for a job based on its size.
 * Smaller files get more retries since they are cheaper and faster to process,
 * while larger files get fewer retries to avoid long-running failed jobs blocking the queue.
 */
export const ATTEMPTS = {
  small: 3,   // Small files (<100MB) can be retried up to 3 times
  medium: 2,  // Medium files (100MB–500MB) can be retried up to 2 times
  large: 1,   // Large files (>500MB) are retried only once
};

/**
 * Represents the various states a file processing job can be in.
 *
 * - 'queued'     : Job is waiting in the queue for processing.
 * - 'processing' : Job is actively being processed (e.g., downloading or encoding).
 * - 'processed'  : Job has completed processing but may not be uploaded yet.
 * - 'uploading'  : Job output is being uploaded to storage.
 * - 'completed'  : Job finished successfully and uploaded.
 * - 'retrying'   : Job failed but is being retried according to attempts rules.
 * - 'failed'     : Job failed permanently after all retries, or moved to DLQ.
 */
export type JobStatus =
  | 'queued'
  | 'processing'
  | 'processed'
  | 'uploading'
  | 'completed'
  | 'retrying'
  | 'failed';