/**
 * Defines the structure of a file processing job in FileWall
 */
export interface FileJob {
  /** Unique identifier for the job/file */
  fileId: string;

  /** Source URL or file path */
  inputUrl: string;

  /** Output key / destination for processed file */
  outputKey: string;

  /** Type of the file */
  fileType: 'video' | 'pdf' | 'zip' | 'other';

  /** Size of the file in bytes */
  size: number;

  /** Tier of the user uploading the file */
  userTier: 'free' | 'premium' | 'vip';
}