import { FileJob } from '../types';
import { processVideo } from '../processors/video';
import { download, upload } from '../utils/r2';
import { acquire, release } from './admission';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { connection } from '../queue/connection';

function getType(size: number): 'small' | 'medium' | 'large' {
  const MB = 1024 * 1024;
  if (size < 50 * MB) return 'small';
  if (size < 500 * MB) return 'medium';
  return 'large';
}

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

// Store job state (temporary DB substitute)
async function setJobMeta(id: string, data: any) {
  await connection.hset(`job:${id}`, data);
  await connection.expire(`job:${id}`, 60 * 60 * 24); // 24h
}

export async function handleJob(job: FileJob, bullJob?: any) {
  const startTime = Date.now();
  const type = getType(job.size);

  let retries = 0;

  // WAIT FOR SLOT
  const acquired = await acquire(type);

  if (!acquired) {
    throw new Error('No capacity, retrying...');
  }

  const tempDir = path.join(os.tmpdir(), job.fileId);
  const inputPath = path.join(tempDir, 'input');
  const outputPath = path.join(tempDir, 'output.mp4');

  await fs.promises.mkdir(tempDir, { recursive: true });

  try {
    await setJobMeta(job.fileId, {
      status: 'processing',
      stage: 'downloading',
      startedAt: startTime,
    });

    await download(job.inputUrl, inputPath);

    if (job.fileType === 'video') {
      await setJobMeta(job.fileId, { stage: 'processing' });

      await processVideo(inputPath, outputPath, async (p: number) => {
        await setJobMeta(job.fileId, {
          stage: 'processing',
          progress: p,
        });

        bullJob?.updateProgress(p);
      });
    }

    await setJobMeta(job.fileId, { stage: 'uploading' });

    const result = await upload(outputPath, job.outputKey);

    await setJobMeta(job.fileId, {
      status: 'completed',
      stage: 'done',
      completedAt: Date.now(),
      duration: Date.now() - startTime,
      output: result, // no fallback needed
      success: true,
    });

  } catch (err: any) {
const isRetrying =
  bullJob && bullJob.attemptsMade < (bullJob.opts.attempts || 1);

  await setJobMeta(job.fileId, {
    status: isRetrying ? 'retrying' : 'failed',
    error: err.message,
    failedAt: Date.now(),
    attemptsMade: bullJob?.attemptsMade || 0,
    maxAttempts: bullJob?.opts.attempts || 1,
    success: false,
  });

    throw err;
  } finally {
    await release(type);
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
}