import { spawn } from 'child_process';
import path from 'path';

export function processVideo(
  input: string,
  output: string,
  onProgress?: (progress: number) => void
): Promise<void> {
  return new Promise((resolve, reject) => {
    const watermark = path.resolve(__dirname, '../../assets/watermark.png');

    const args = [
      '-y',
      '-i', input,
      '-i', watermark,
      '-filter_complex', 'scale=-2:360[video];[video][1]overlay=10:10',
      '-c:v', 'libx264',
      '-preset', 'fast',
      '-crf', '28',
      '-c:a', 'aac',
      '-progress', 'pipe:2',
      
      //Limit ffmpeg threads 
      '-threads', '2',  
      output,
    ];

    const ffmpeg = spawn('ffmpeg', args);

    ffmpeg.stderr.on('data', (data) => {
      const str = data.toString();

      if (onProgress && str.includes('out_time_ms')) {
        const time = Number(str.split('=')[1]);
        onProgress(time);
      }
    });

    const timeout = setTimeout(() => {
      ffmpeg.kill('SIGKILL');
      reject(new Error('FFmpeg timeout'));
    }, 10 * 60 * 1000);

    ffmpeg.on('close', (code) => {
      clearTimeout(timeout);

      if (code !== 0) return reject(new Error('FFmpeg failed'));
      resolve();
    });
  });
}