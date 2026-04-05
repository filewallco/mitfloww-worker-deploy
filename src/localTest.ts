import fs from 'fs';
import path from 'path';
import { enqueueFile } from './queue/enqueue';
import { randomUUID } from 'crypto';
import { FileJob } from './types';

const TEST_DIR = path.join(__dirname, '../test-files');

/**
 * Track already processed files
 */
const seen = new Set<string>();

/**
 * Safe enqueue wrapper
 */
async function enqueueSafe(file: string) {
  if (seen.has(file)) return;

  seen.add(file);

  const fullPath = path.join(TEST_DIR, file);
  const stats = fs.statSync(fullPath);
  const ext = path.extname(file).toLowerCase();

  if (!['.mp4', '.mov', '.avi', '.mkv'].includes(ext)) {
    console.log(`Skipping non-video: ${file}`);
    return;
  }

  const job: FileJob = {
    fileId: randomUUID(),
    inputUrl: `file://${fullPath}`,
    outputKey: `local/${file}`,
    fileType: 'video',
    size: stats.size,
    userTier: 'free',
  };

  console.log(`ENQUEUE CALLED: ${file}`);
  await enqueueFile(job);
}

/**
 * Initial scan (startup)
 */
async function initialScan() {
  const files = fs.readdirSync(TEST_DIR);
  console.log(`Initial scan: ${files.length} files`);

  for (const file of files) {
    await enqueueSafe(file);
  }
}

/**
 * NEW: Polling loop for new files
 */
function startPolling() {
  setInterval(async () => {
    const files = fs.readdirSync(TEST_DIR);

    for (const file of files) {
      if (!seen.has(file)) {
        console.log('NEW FILE DETECTED:', file);
        await enqueueSafe(file);
      }
    }
  }, 3000); // every 3 seconds
}

/**
 * Entry
 */
async function run() {
  await initialScan();
  startPolling();
}

run();