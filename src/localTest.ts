import fs from 'fs';
import path from 'path';
import { enqueueFile } from './queue/enqueue';
import { randomUUID } from 'crypto';
import { FileJob } from './types';

const TEST_DIR = path.join(__dirname, '../test-files');

async function run() {
  if (!fs.existsSync(TEST_DIR)) {
    console.error('test-files folder not found');
    process.exit(1);
  }

  const files = fs.readdirSync(TEST_DIR);

  if (files.length === 0) {
    console.log('No files found in test-files/');
    return;
  }

  console.log(`Found ${files.length} files`);

  for (const file of files) {
    const fullPath = path.join(TEST_DIR, file);
    const stats = fs.statSync(fullPath);
    const ext = path.extname(file).toLowerCase();
    
    if (!['.mp4', '.mov', '.avi', '.mkv'].includes(ext)) {
      console.log(`⏭ Skipping non-video: ${file}`);
      continue;
    }

    const job: FileJob = {
      fileId: randomUUID(),
      inputUrl: `file://${fullPath}`,
      outputKey: `local/${file}`,
      fileType: 'video',
      size: stats.size,
      userTier: 'free',
    };

    console.log(`Enqueueing: ${file}`);

    await enqueueFile(job);
  }

  console.log('All jobs queued');
}

run();