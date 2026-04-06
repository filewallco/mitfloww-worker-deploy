import { spawn, execSync  } from 'child_process';
import path from 'path';
import { config } from '../config';

/**
 * Processes a video by scaling it to 360p and overlaying a watermark image.
 * Uses FFmpeg via a child process.
 * 
 * @param input - Path to the input video file
 * @param output - Path where the processed video will be saved
 * @param onProgress - Optional callback to report progress in milliseconds
 * @returns A promise that resolves when processing is complete or rejects on error
 */
export function processVideo(
  input: string,
  output: string,
  onProgress?: (progress: number) => void
): Promise<void> {
  return new Promise((resolve, reject) => {
    // Resolve watermark path relative to current file
    const watermark = path.resolve(__dirname, '../../assets/watermark.png');

    /**
     * FFmpeg command arguments
     * -y : overwrite output file if exists
     * -i input : input video file
     * -i watermark : watermark image to overlay
     * -filter_complex : video filters applied:
     *      scale=-2:360 -> scale video height to 360px, width auto-adjusted to maintain aspect ratio
     *      overlay=10:10 -> overlay watermark at 10px from top-left corner
     * -c:v libx264 : encode video using H.264
     * -preset fast : faster encoding speed
     * -crf 28 : quality/compression ratio (higher = lower quality)
     * -c:a aac : encode audio using AAC
     * -progress pipe:2 : report progress on stderr
     * -threads 2 : limit number of CPU threads FFmpeg uses
     * output : path to save final processed video
     */
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
      '-threads', '1',
      output,
    ];

    // Spawn FFmpeg process
    const ffmpeg = spawn(config.ffmpegPath, args);

    /**
     * Listen for progress updates from FFmpeg
     * FFmpeg outputs progress info to stderr with 'out_time_ms'
     * Convert time to number and call onProgress callback
     */
    ffmpeg.stderr.on('data', (data) => {
      const str = data.toString();

      if (onProgress && str.includes('out_time_ms')) {
        const time = Number(str.split('=')[1]);
        onProgress(time);
      }
    });

    /**
     * Set a timeout for the FFmpeg process
     * If processing exceeds 10 minutes, kill the process and reject
     */
    const timeout = setTimeout(() => {
      ffmpeg.kill('SIGKILL');
      reject(new Error('FFmpeg timeout'));
    }, 10 * 60 * 1000); // 10 minutes

    /**
     * Handle FFmpeg process exit
     * Clear timeout and resolve/reject promise based on exit code
     */
    ffmpeg.on('close', (code) => {
      clearTimeout(timeout);

      if (code !== 0) return reject(new Error('FFmpeg failed'));
      resolve();
    });
  });
}

export function getDuration(file: string): number {
  const out = execSync(
    `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${file}"`
  );

  return parseFloat(out.toString()) * 1000;
}