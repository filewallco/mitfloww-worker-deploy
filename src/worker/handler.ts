import { FileJob } from '../types';
import { processVideo } from '../processors/video';
import { download, upload } from '../utils/r2';
import path from 'path';
import fs from 'fs';
import os from 'os'; // for windows

export async function handleJob(job: FileJob) {
  console.log('Downloading:', job.inputUrl);
  
// for linux
//   const inputPath = path.join('/tmp', `${job.fileId}-input`);
//   const outputPath = path.join('/tmp', `${job.fileId}-output.mp4`);

// for windows
const tempDir = os.tmpdir();
const inputPath = path.join(tempDir, `${job.fileId}-input`);
const outputPath = path.join(tempDir, `${job.fileId}-output.mp4`);


  await download(job.inputUrl, inputPath);

  if (job.fileType === 'video') {
    await processVideo(inputPath, outputPath);
  }

  await upload(outputPath, job.outputKey);

  fs.unlinkSync(inputPath);
  fs.unlinkSync(outputPath);
}