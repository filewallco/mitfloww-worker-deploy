import fs from 'fs';
import path from 'path';
import { enqueueFile } from './queue/enqueue';
import { randomUUID } from 'crypto';
import { FileJob } from './types';
import { FILE_TYPE } from './constants';
import { fileTypeFromFile } from 'file-type';

const TEST_DIR = path.join(__dirname, '../test-files');

/**
 * Local in-memory dedup (fast)
 */
const seen = new Set<string>();

/**
 * Redis-based dedup (persistent across restarts)
 */
import { connection } from './queue/connection';

const FILE_SEEN_KEY = (file: string) => `file:seen:${file}`;

/**
 * Enqueue with STRONG deduplication
 *
 * Guarantees:
 * - No duplicate enqueue (even across restarts)
 * - Safe against polling + race conditions
 */
async function enqueueSafe(file: string) {
  if (file.startsWith('.')) return; // skip .gitkeep etc

  if (seen.has(file)) return;

  /**
   * Redis SET with NX + EX (type-safe for ioredis)
   *
   * IMPORTANT:
   * ioredis expects options in a specific order:
   *   SET key value EX seconds NX
   */
  const already = await connection.set(
    FILE_SEEN_KEY(file),
    '1',
    'EX',
    60 * 60,
    'NX'
  );

  if (!already) return;

  const fullPath = path.join(TEST_DIR, file);

  if (!fs.existsSync(fullPath)) return;

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
    return;
  }

  const outputKey =
    fileType === FILE_TYPE.IMAGE
      ? `local/${path.parse(file).name}${path.extname(file)}`
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
  const files = fs.readdirSync(TEST_DIR).filter(f => !f.startsWith('.'));
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
      if (file.startsWith('.')) continue;

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