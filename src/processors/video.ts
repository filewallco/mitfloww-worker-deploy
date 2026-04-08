import { spawn, execSync } from 'child_process';
import path from 'path';
import { config } from '../config';
import os from 'os';

/**
 * Compute FFmpeg thread allocation.
 * Strategy:
 * - Avoid CPU oversubscription
 * - Scale with global CPU limit
 */
function getFfmpegThreads(): number {
  const globalLimit = Number(process.env.GLOBAL_CPU_LIMIT || os.cpus().length);

  // Divide CPU across expected parallel jobs
  return Math.max(1, Math.floor(globalLimit / 4));
}

/**
 * Executes FFmpeg to process a video:
 * - Scales to 360p (maintaining aspect ratio)
 * - Applies watermark overlay
 * - Encodes using H.264 (libx264)
 *
 * Design goals:
 * - Non-blocking (child process)
 * - Observable (progress reporting)
 * - Fault-tolerant (stall + max runtime protection)
 *
 * @param input Absolute path to input video file
 * @param output Absolute path for processed output file
 * @param onProgress Optional callback receiving processed timestamp (ms)
 *
 * @returns Promise<void> resolved on success, rejected on failure
 */
export function processVideo(
  input: string,
  output: string,
  options?: {
    start?: number;
    duration?: number;
    totalDuration?: number; // FULL video duration (important)
    progressOffset?: number; // where this chunk starts in %
    progressScale?: number;  // how much % this chunk contributes
  },
  onProgress?: (progress: number) => void
): Promise<void> {
  return new Promise((resolve, reject) => {
    const watermark = path.resolve(__dirname, '../../assets/watermark.png');

    const args = [
      '-y',

      // FAST SEEK (important for large videos)
      ...(options?.start ? ['-ss', String(options.start)] : []),

      '-i', input,
      '-i', watermark,

      ...(options?.duration ? ['-t', String(options.duration)] : []),

      '-filter_complex', 'scale=-2:360[video];[video][1]overlay=10:10',
      '-c:v', 'libx264',
      '-preset', 'fast',
      '-crf', '28',
      '-c:a', 'aac',
      '-movflags', '+faststart',
      '-progress', 'pipe:2',
      '-threads',  String(getFfmpegThreads()),
      output,
    ];

    const ffmpeg = spawn(config.ffmpegPath, args);

    let lastProgressTime = Date.now();
    let buffer = '';
    let finished = false;

    function cleanup() {
      clearInterval(stallCheck);
      clearTimeout(hardTimeout);
    }

    function safeReject(err: Error) {
      if (finished) return;
      finished = true;
      cleanup();
      reject(err);
    }

    function safeResolve() {
      if (finished) return;
      finished = true;
      cleanup();
      resolve();
    }

    ffmpeg.stderr.on('data', (data) => {
      console.error('[FFMPEG]', data.toString());
      buffer += data.toString();

      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (line.startsWith('out_time_ms=')) {
          lastProgressTime = Date.now();

          if (onProgress) {
            const value = Number(line.split('=')[1]);

            if (!isNaN(value)) {
              if (options?.totalDuration) {
                // progressive mode
                const chunkProgress =
                  value / (options.duration ? options.duration * 1000 : options.totalDuration);
                const scaled =
                  (options.progressOffset || 0) +
                  chunkProgress * (options.progressScale || 100);
                var percentage = Math.min((scaled / 100) * 100, 100) ?? 100;
                onProgress(percentage);
              } else {
                // fallback (old behavior)
                onProgress(value);const percent = Math.min(
                  (value / (options?.totalDuration || 1)) * 100,
                  100
                );

                onProgress(percent);
              }
            }
          }
        }
      }
    });

    // STALL DETECTION
    const stallCheck = setInterval(() => {
      const STALL_LIMIT = 5 * 60 * 1000;

      if (Date.now() - lastProgressTime > STALL_LIMIT) {
        ffmpeg.kill('SIGKILL');
        safeReject(new Error('FFmpeg stalled'));
      }
    }, 60_000);

    // HARD TIMEOUT
    const MAX_RUNTIME = 6 * 60 * 60 * 1000;

    const hardTimeout = setTimeout(() => {
      ffmpeg.kill('SIGKILL');
      safeReject(new Error('FFmpeg max runtime exceeded'));
    }, MAX_RUNTIME);

    ffmpeg.on('close', (code) => {
      if (code === 0) safeResolve();
      else safeReject(new Error(`FFmpeg failed with code ${code}`));
    });

    ffmpeg.on('error', safeReject);
  });
}

/**
 * Generate a lightweight preview clip (first N seconds)
 * This replaces expensive HLS preview generation.
 */
export function generatePreviewClip(
  input: string,
  output: string,
  seconds: number = 8
): Promise<void> {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn(config.ffmpegPath, [
      '-y',
      '-i', input,

      '-t', String(seconds),

      '-vf', 'scale=-2:360',
      '-pix_fmt', 'yuv420p', // ensures compatibility with more players
      '-c:v', 'libx264',
      '-preset', 'veryfast',
      '-crf', '30',
      '-movflags', '+faststart',
      '-an', // no audio → saves CPU
      '-threads', String(2),

      output,
    ]);

    ffmpeg.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`Preview generation failed: ${code}`));
    });

    ffmpeg.on('error', reject);
  });
}

/**
 * Extracts video duration using ffprobe.
 *
 * Notes:
 * - Returns duration in milliseconds
 * - Used for progress normalization in higher-level logic
 * - Blocking call (acceptable due to short execution time)
 *
 * @param file Absolute file path
 * @returns Duration in milliseconds
 */
export function getDuration(file: string): number {
  const out = execSync(
    `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${file}"`
  );

  return parseFloat(out.toString()) * 1000;
}