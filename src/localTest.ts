import fs from 'fs';
import path from 'path';
import { enqueueFile } from './queue/enqueue';
import { randomUUID } from 'crypto';
import { FileJob } from './types';
import { FILE_TYPE } from './constants';
import { fileTypeFromFile } from 'file-type';

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

  const fullPath = path.join(TEST_DIR, file);
  const stats = fs.statSync(fullPath);
  const detected = await fileTypeFromFile(fullPath);

  if (!detected) {
    console.log(`Skipping unknown file: ${file}`);
    return;
  }

  let fileType: FileJob['fileType'] = FILE_TYPE.OTHER;

  if (detected.mime.startsWith('image/')) {
    fileType = FILE_TYPE.IMAGE;
  } else if (detected.mime.startsWith('video/')) {
    fileType = FILE_TYPE.VIDEO;
  } else {
    console.log(`Unsupported file type: ${detected.mime}`);
    return;
  }

  const extImage = path.extname(file);

  const outputKey =
    fileType === FILE_TYPE.IMAGE
      ? `local/${path.parse(file).name}${extImage}`
      : `local/${file}`;
  
  const job: FileJob = {
    fileId: randomUUID(),
    inputUrl: `file://${fullPath}`,
    outputKey,
    fileType,
    size: stats.size,
    userTier: 'free',
  };

  console.log(`ENQUEUE CALLED: ${file}`);
  await enqueueFile(job);
  seen.add(file);
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