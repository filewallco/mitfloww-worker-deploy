import fs from 'fs';
import { config } from '../config';

type DiskStats = {
  freeBytes: number;
  totalBytes: number;
};

function statFsSafe(): any {
  return (fs as any).statfsSync(config.tempDir);
}

export function getDiskStats(): DiskStats {
  const stat = statFsSafe();
  const freeBytes = Number(stat.bavail) * Number(stat.bsize);
  const totalBytes = Number(stat.blocks) * Number(stat.bsize);
  return { freeBytes, totalBytes };
}

export function getFreeDiskSpace(): number {
  return getDiskStats().freeBytes;
}

export function getDiskCapacity(): number {
  return getDiskStats().totalBytes;
}
