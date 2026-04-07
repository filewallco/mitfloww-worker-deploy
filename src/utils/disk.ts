import fs from 'fs';
import { config } from '../config';

/**
 * Returns available disk space (bytes) for temp directory.
 * Uses statfs (Node 18+).
 */
export function getFreeDiskSpace(): number {
  const stat = (fs as any).statfsSync(config.tempDir);

  return stat.bavail * stat.bsize; // available blocks * block size
}