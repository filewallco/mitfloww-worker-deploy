import fs from 'fs';
import path from 'path';
import { config } from '../config';
import { getFreeDiskSpace } from './disk';

/**
 * Deletes oldest temp folders until disk is healthy.
 */
export async function cleanupTempDir() {
  const dir = config.tempDir;

  if (!fs.existsSync(dir)) return;

  const entries = await fs.promises.readdir(dir, { withFileTypes: true });

  const folders = await Promise.all(
    entries
      .filter(e => e.isDirectory())
      .map(async (e) => {
        const full = path.join(dir, e.name);
        const stat = await fs.promises.stat(full);
        return { path: full, time: stat.mtimeMs };
      })
  );

  // Oldest first
  folders.sort((a, b) => a.time - b.time);

  let free = getFreeDiskSpace();

  for (const folder of folders) {
    if (free >= config.disk.targetFreeBytes) break;

    try {
      await fs.promises.rm(folder.path, { recursive: true, force: true });
      free = getFreeDiskSpace();
      console.log(`Deleted temp: ${folder.path}`);
    } catch {}
  }
}