import { execFileSync, spawn } from 'child_process';
import path from 'path';
import { config } from '../config';
import os from 'os';
import { createFfmpegStderrBuffer, logger } from '../utils/logger';

type FfprobeStream = {
  codec_type?: string;
  codec_name?: string;
  width?: number;
  height?: number;
  disposition?: {
    default?: number;
  };
};

type FfprobeFormat = {
  format_name?: string;
  duration?: string;
};

type FfprobePayload = {
  streams?: FfprobeStream[];
  format?: FfprobeFormat;
};

export type VideoProbe = {
  formatName: string | null;
  durationMs: number | null;
  hasVideo: boolean;
  hasAudio: boolean;
  videoCodec: string | null;
  audioCodec: string | null;
  width: number | null;
  height: number | null;
};

const ALLOWED_VIDEO_CODECS = new Set([
  'h264',
  'hevc',
  'vp8',
  'vp9',
  'av1',
  'mpeg4',
  'mjpeg'
]);
const ALLOWED_AUDIO_CODECS = new Set([
  'aac',
  'mp3',
  'opus',
  'vorbis',
  'pcm_s16le',
  'ac3',
  'eac3'
]);

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

function clampProgress(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(value, 100));
}

const FFPROBE_PROTOCOL_ARGS = [
  '-protocol_whitelist',
  'file,pipe,data'
];
const LOCAL_INPUT_ARGS = [
  '-protocol_whitelist',
  'file,pipe,data'
];

function runFfprobe(file: string): FfprobePayload {
  const out = execFileSync(config.ffprobePath, [
    '-v',
    'error',
    '-show_entries',
    'format=format_name,duration:stream=codec_type,codec_name,width,height,disposition',
    '-of',
    'json',
    ...FFPROBE_PROTOCOL_ARGS,
    file,
  ]);

  return JSON.parse(out.toString()) as FfprobePayload;
}

function pickPrimaryStream(
  streams: FfprobeStream[],
  codecType: 'video' | 'audio'
): FfprobeStream | undefined {
  const matches = streams.filter((stream) => stream.codec_type === codecType);

  if (matches.length === 0) return undefined;

  return matches.find((stream) => stream.disposition?.default === 1) ?? matches[0];
}

export function assertAllowedVideoProbe(probe: VideoProbe): void {
  const videoCodec = probe.videoCodec?.toLowerCase() ?? null;
  const audioCodec = probe.audioCodec?.toLowerCase() ?? null;

  if (!videoCodec || !ALLOWED_VIDEO_CODECS.has(videoCodec)) {
    throw new Error(`Unsupported video codec: ${probe.videoCodec || 'unknown'}`);
  }

  if (probe.hasAudio && (!audioCodec || !ALLOWED_AUDIO_CODECS.has(audioCodec))) {
    throw new Error(`Unsupported audio codec: ${probe.audioCodec || 'unknown'}`);
  }
}

export function inspectVideoInput(file: string): VideoProbe {
  const payload = runFfprobe(file);
  const streams = payload.streams ?? [];
  const videoStream = pickPrimaryStream(streams, 'video');
  const audioStream = pickPrimaryStream(streams, 'audio');
  const durationSeconds = Number(payload.format?.duration ?? NaN);

  return {
    formatName: payload.format?.format_name ?? null,
    durationMs: Number.isFinite(durationSeconds) ? durationSeconds * 1000 : null,
    hasVideo: Boolean(videoStream),
    hasAudio: Boolean(audioStream),
    videoCodec: videoStream?.codec_name ?? null,
    audioCodec: audioStream?.codec_name ?? null,
    width: videoStream?.width ?? null,
    height: videoStream?.height ?? null,
  };
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
    jobId?: string; // optional job id for better diagnostics
  },
  onProgress?: (progress: number) => void
): Promise<void> {
  return new Promise((resolve, reject) => {
    const watermark = path.resolve(__dirname, '../../assets/watermark.png');

    const args = [
      '-y',
      '-nostdin',

      // FAST SEEK (important for large videos)
      ...(options?.start ? ['-ss', String(options.start)] : []),

      ...LOCAL_INPUT_ARGS,
      '-fflags', '+genpts',
      '-i', input,
      ...LOCAL_INPUT_ARGS,
      '-i', watermark,

      ...(options?.duration ? ['-t', String(options.duration)] : []),

      '-filter_complex', '[0:v:0]scale=-2:360[video];[video][1:v:0]overlay=10:10[vout]',
      '-map', '[vout]',
      '-map', '0:a:0?',
      '-sn',
      '-dn',
      '-c:v', 'libx264',
      '-preset', 'fast',
      '-crf', '28',
      '-pix_fmt', 'yuv420p',
      '-c:a', 'aac',
      '-movflags', '+faststart',
      '-progress', 'pipe:2',
      '-threads',  String(getFfmpegThreads()),
      output,
    ];

    const ffmpeg = spawn(config.ffmpegPath, args);

    let lastProgressTime = Date.now();
    let progressBuffer = '';
    let finished = false;
    const stderrBuffer = createFfmpegStderrBuffer(50);

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
      const str = data.toString();
      // preserve progress data separately
      progressBuffer += str;

      const lines = progressBuffer.split('\n');
      progressBuffer = lines.pop() || '';

      for (const line of lines) {
        // track non-progress stderr lines in rolling buffer
        if (!line.startsWith('out_time_ms=')) {
          stderrBuffer.push(line);
        } else {
          lastProgressTime = Date.now();

          if (onProgress) {
            const value = Number(line.split('=')[1]);

            if (!isNaN(value)) {
              const processedMs = value / 1000;

              if (options?.totalDuration) {
                const durationMs =
                  options.duration ? options.duration * 1000 : options.totalDuration;
                const chunkProgress =
                  durationMs > 0 ? processedMs / durationMs : 0;
                const scaled =
                  (options.progressOffset || 0) +
                  chunkProgress * (options.progressScale || 100);

                onProgress(clampProgress(scaled));
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
        const err = new Error('FFmpeg stalled');
        logger.error('FFmpeg stalled', { jobId: options?.jobId, input, output, lastStderr: stderrBuffer.getLines() });
        ffmpeg.kill('SIGKILL');
        safeReject(err);
      }
    }, 60_000);

    // HARD TIMEOUT
    const MAX_RUNTIME = 6 * 60 * 60 * 1000;

    const hardTimeout = setTimeout(() => {
      const err = new Error('FFmpeg max runtime exceeded');
      logger.error('FFmpeg max runtime exceeded', { jobId: options?.jobId, input, output, lastStderr: stderrBuffer.getLines() });
      ffmpeg.kill('SIGKILL');
      safeReject(err);
    }, MAX_RUNTIME);

    ffmpeg.on('close', (code) => {
      if (code === 0) safeResolve();
      else {
        logger.error('FFmpeg failed', { jobId: options?.jobId, exitCode: code, input, output, lastStderr: stderrBuffer.getLines() });
        safeReject(new Error(`FFmpeg failed with code ${code}`));
      }
    });

    ffmpeg.on('error', (err) => {
      logger.error('FFmpeg spawn error', { jobId: options?.jobId, error: err, input, output, lastStderr: stderrBuffer.getLines() });
      safeReject(err);
    });
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
      '-nostdin',
      ...LOCAL_INPUT_ARGS,
      '-fflags', '+genpts',
      '-i', input,
      '-map', '0:v:0',
      '-sn',
      '-dn',

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
  const probe = inspectVideoInput(file);
  return probe.durationMs ?? 0;
}
