import fs from 'fs';
import os from 'os';

/**
 * Returns available disk space (bytes) for temp directory.
 * Uses statfs (Node 18+).
 */
export function getFreeDiskSpace(): number {
  const stat = (fs as any).statfsSync(os.tmpdir());

  return stat.bavail * stat.bsize; // available blocks * block size
}