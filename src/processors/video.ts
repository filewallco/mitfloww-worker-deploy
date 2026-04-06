import { spawn, execSync } from 'child_process';
import path from 'path';
import { config } from '../config';

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
  onProgress?: (progress: number) => void
): Promise<void> {
  return new Promise((resolve, reject) => {

    /** Absolute path to watermark asset */
    const watermark = path.resolve(__dirname, '../../assets/watermark.png');

    /**
     * FFmpeg arguments:
     * - progress pipe used for real-time progress tracking
     * - single-threaded to allow controlled concurrency at system level
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

    /** Spawn FFmpeg child process */
    const ffmpeg = spawn(config.ffmpegPath, args);

    /** Timestamp of last progress update (used for stall detection) */
    let lastProgressTime = Date.now();

    /** Buffer for handling partial stderr chunks (line-safe parsing) */
    let buffer = '';

    /** Prevent multiple resolve/reject calls */
    let finished = false;

    /**
     * Cleanup all timers to avoid leaks and ghost executions
     */
    function cleanup() {
      clearInterval(stallCheck);
      clearTimeout(hardTimeout);
    }

    /**
     * Safe reject wrapper (idempotent)
     */
    function safeReject(err: Error) {
      if (finished) return;
      finished = true;
      cleanup();
      reject(err);
    }

    /**
     * Safe resolve wrapper (idempotent)
     */
    function safeResolve() {
      if (finished) return;
      finished = true;
      cleanup();
      resolve();
    }

    /**
     * STDERR parsing:
     * FFmpeg emits key=value pairs, but in chunked form.
     * We buffer and process line-by-line to avoid partial parsing bugs.
     */
    ffmpeg.stderr.on('data', (data) => {
      buffer += data.toString();

      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (line.startsWith('out_time_ms=')) {
          lastProgressTime = Date.now();

          if (onProgress) {
            const value = Number(line.split('=')[1]);
            if (!isNaN(value)) {
              onProgress(value);
            }
          }
        }
      }
    });

    /**
     * Stall detection:
     * If no progress is observed for a defined window,
     * assume FFmpeg is stuck (bad input / codec / deadlock)
     */
    const stallCheck = setInterval(() => {
      const STALL_LIMIT = 5 * 60 * 1000; // 5 minutes

      if (Date.now() - lastProgressTime > STALL_LIMIT) {
        ffmpeg.kill('SIGKILL');
        safeReject(new Error('FFmpeg stalled'));
      }
    }, 60_000);

    /**
     * Hard runtime limit:
     * Safety net for cases where FFmpeg keeps running without emitting progress
     * (e.g., corrupted streams, edge codecs)
     */
    const MAX_RUNTIME = 6 * 60 * 60 * 1000; // 6 hours

    const hardTimeout = setTimeout(() => {
      ffmpeg.kill('SIGKILL');
      safeReject(new Error('FFmpeg max runtime exceeded'));
    }, MAX_RUNTIME);

    /**
     * Process exit handler
     */
    ffmpeg.on('close', (code) => {
      if (code === 0) {
        safeResolve();
      } else {
        safeReject(new Error(`FFmpeg failed with code ${code}`));
      }
    });

    /**
     * Spawn-level failure (binary missing, permission issues, etc.)
     */
    ffmpeg.on('error', (err) => {
      safeReject(err);
    });
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