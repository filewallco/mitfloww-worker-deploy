import fs from 'fs';
import path from 'path';
import { enqueueFile } from './queue/enqueue';
import { randomUUID } from 'crypto';
import { FileJob } from './types';
import { FILE_TYPE } from './constants';
import { fileTypeFromFile } from 'file-type';
import { classifyFileType } from './utils/media';

const TEST_DIR = path.join(__dirname, '../test-files');

/**
 * Local in-memory dedup (fast)
 * Keeps track of files already processed in this runtime
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
    FILE_SEEN_KEY(file), // Redis key
    '1', // value
    'EX', // expiry type = seconds
    60 * 60, // TTL
    'NX' // only set the key if it does NOT already exist
  );

  if (!already) return;

  const fullPath = path.join(TEST_DIR, file);

  if (!fs.existsSync(fullPath)) return;

  const stats = fs.statSync(fullPath); // Get file size
  const detected = await fileTypeFromFile(fullPath).catch(() => undefined);
  const fileType = detected
    ? classifyFileType(detected.mime, `sample.${detected.ext}`)
    : FILE_TYPE.OTHER;

  if (fileType !== FILE_TYPE.IMAGE && fileType !== FILE_TYPE.VIDEO && fileType !== FILE_TYPE.PDF) {
    console.log(`Skipping unsupported file: ${file}`);
    return;
  }

  const outputKey =
    fileType === FILE_TYPE.IMAGE || fileType === FILE_TYPE.PDF
      ? `local/${path.parse(file).name}${path.extname(file)}`
      : `local/${file}`;
  const job: FileJob = {
    fileId: randomUUID(),
    inputUrl: `file://${fullPath}`,
    outputKey,
    fileType,
    size: stats.size,
    userTier: 'free',
    userId: 'local-user',
    batchId: 'local-batch',
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
 * Polling loop for new files
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
