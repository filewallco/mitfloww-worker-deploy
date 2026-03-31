export interface FileJob {
  fileId: string;
  inputUrl: string;
  outputKey: string;
  fileType: 'video' | 'pdf' | 'zip' | 'other';
  size: number;
}