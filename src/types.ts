/**
 * This file defines the core types and constants used throughout the file processing system. It includes:
 * - FileJob: The main interface representing a file processing job, containing all relevant metadata and user information.
 * - ATTEMPTS: A constant object defining the maximum number of processing attempts based on user tiers.
 * - JobStatus: A type representing the various states a file processing job can be in, allowing for clear tracking of the job's lifecycle.
 * These definitions are crucial for ensuring type safety and consistency across the application, especially when handling file processing logic, user management, and job status tracking.
 */
export interface FileJob {
  // Unique identifiers for the file and its version (if applicable)
  fileId: string;
  // Optional version ID for files that support versioning (e.g., R2)
  fileVersionId?: string;

  // Original URL of the file to be processed (if applicable)
  inputUrl?: string;
  // Key or path where the processed file will be stored
  outputKey: string;

  // Optional fields for tracking the source and destination of the file, especially when using remote storage like R2
  sourceBucket?: string;
  // Optional key for the original file in the source bucket (useful for R2 or similar services)
  sourceKey?: string;
  // Optional bucket name for the output file in remote storage scenarios
  outputBucket?: string;
  // Optional key for the output file in remote storage scenarios
  logKey?: string;

  // Optional fields for file metadata, which can be used for processing decisions or logging
  fileName?: string;
  // Optional original name of the file, which can be useful for logging or user-facing messages
  originalName?: string;
  // Optional MIME type of the file, which can help determine how to process it
  mimeType?: string;
  // Optional file extension, which can also assist in processing decisions
  extension?: string;

  // Determined file type category based on MIME type or extension, used for processing logic
  fileType: "video" | "image" | "pdf" | "zip" | "other";
  // Size of the file in bytes, which is crucial for validating against size limits and managing resources
  size: number;

  // User information for tracking and access control
  userTier: "free" | "premium" | "vip";
  // Unique identifier for the user who submitted the file processing job
  userId: string;
  // Optional email of the user, which can be used for notifications or logging
  userEmail?: string | null;
  // Optional username of the user, which can also be used for logging or user-facing messages
  userName?: string | null;

  // Optional fields for batch processing, allowing multiple files to be processed together and tracked as a group
  batchId?: string;
  // Optional index of the file within a batch, useful for tracking progress and results in batch processing scenarios
  retryCount?: number;

  // Optional fields for callback functionality, allowing the system to notify external services upon job completion or failure
  callbackUrl?: string;
  // Optional token for authenticating callback requests, ensuring that only authorized services can receive notifications
  callbackToken?: string;

  /**
   * Compress to 360p and watermark.
   * When false, only watermark is applied.
   */
  isLargeFile?: boolean;

  /**
   * Generate preview assets.
   * Admin-only feature.
   */
  isPreviewGeneration?: boolean;
}

/**
 * ATTEMPTS defines the maximum number of processing attempts for each user tier. After exceeding the limit, the job will be marked as "failed" and won't be retried further.
 * - Free users get 3 attempts, allowing for some retries in case of transient issues.
 * - Premium users get 2 attempts, providing a balance between leniency and resource management.
 * - VIP users get only 1 attempt, prioritizing resource allocation and encouraging them to ensure their files meet the requirements before processing.
 * This structure helps manage resources effectively while providing a better experience for higher-tier users.
 */
export const ATTEMPTS = {
  small: 3,
  medium: 2,
  large: 1,
};

/**
 * JobStatus represents the current state of a file processing job. The typical flow is:
 * "queued" -> "processing" -> "processed" -> "uploading" -> "completed"
 * If any step fails, it can transition to "retrying" (if attempts remain) or "failed" (if no attempts left).
 */
export type JobStatus =
  | "queued"
  | "processing"
  | "processed"
  | "uploading"
  | "completed"
  | "retrying"
  | "failed"
  | "cancelled";
