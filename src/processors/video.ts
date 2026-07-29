import { execFileSync, spawn } from 'child_process';
import fs from 'fs';
import os from 'os';
import { config } from '../config';
import { createFfmpegStderrBuffer, logger } from '../utils/logger';
import {
  createRepeatedWatermarkOverlayFile,
  getDefaultWatermarkText,
} from '../utils/watermark';

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
  'mjpeg',
]);

const ALLOWED_AUDIO_CODECS = new Set([
  'aac',
  'mp3',
  'opus',
  'vorbis',
  'pcm_s16le',
  'ac3',
  'eac3',
]);

function getFfmpegThreads(): number {
  const globalLimit = Number(process.env.GLOBAL_CPU_LIMIT || os.cpus().length);
  return Math.max(1, Math.floor(globalLimit / 4));
}

function clampProgress(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(value, 100));
}

function evenDimension(value: number | null | undefined, fallback: number) {
  const raw = Number.isFinite(value) && value && value > 0 ? Number(value) : fallback;
  return Math.max(2, Math.floor(raw / 2) * 2);
}

const FFPROBE_PROTOCOL_ARGS = [
  '-protocol_whitelist',
  'file,pipe,data',
];

const LOCAL_INPUT_ARGS = [
  '-protocol_whitelist',
  'file,pipe,data',
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
  codecType: 'video' | 'audio',
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

export async function processVideo(
  input: string,
  output: string,
  options?: {
    start?: number;
    duration?: number;
    totalDuration?: number;
    progressOffset?: number;
    progressScale?: number;
    jobId?: string;
    width?: number | null;
    height?: number | null;
    watermarkText?: string;
    isLargeFile?: boolean;
  },
  onProgress?: (progress: number) => void,
): Promise<void> {
  const outputWidth = evenDimension(options?.width, 1280);
  const outputHeight = evenDimension(options?.height, 720);
  const isLargeFile = options?.isLargeFile ?? false;

  const overlayPath = await createRepeatedWatermarkOverlayFile(
    options?.jobId || 'video',
    {
      width: outputWidth,
      height: outputHeight,
      text: options?.watermarkText || getDefaultWatermarkText(),
      opacity: Number(process.env.VIDEO_WATERMARK_OPACITY || 0.14),
      density: 'light',
    },
  );

  return new Promise((resolve, reject) => {
    const args = [
      '-y',
      '-nostdin',

      ...(options?.start ? ['-ss', String(options.start)] : []),

      ...LOCAL_INPUT_ARGS,
      '-threads',
      String(getFfmpegThreads()),
      '-fflags',
      '+genpts',
      '-i',
      input,

      ...LOCAL_INPUT_ARGS,
      '-i',
      overlayPath,

      ...(options?.duration ? ['-t', String(options.duration)] : []),

      /**
       * Keep original resolution but force even dimensions for libx264.
       * Overlay is full-canvas and low opacity, so review remains usable.
       */
      '-filter_complex',
      isLargeFile
        ? `[0:v:0]scale=-2:360:flags=fast_bilinear[base];[base][1:v:0]overlay=0:0[vout]`
        : `[0:v:0]scale='min(iw,${outputWidth})':'min(ih,${outputHeight})':flags=fast_bilinear[base];[base][1:v:0]overlay=0:0[vout]`,

      '-map',
      '[vout]',
      '-map',
      '0:a:0?',
      '-sn',
      '-dn',

      '-c:v',
      'libx264',
      '-preset',
      isLargeFile ? 'ultrafast' : 'fast',
      ...(isLargeFile ? ['-tune', 'fastdecode'] : []),
      '-crf',
      isLargeFile ? '28' : '18',
      '-pix_fmt',
      'yuv420p',

      '-c:a',
      'aac',
      '-movflags',
      '+faststart',

      '-progress',
      'pipe:2',
      '-threads',
      String(getFfmpegThreads()),

      output,
    ];

    const ffmpeg = spawn(config.ffmpegPath, args);

    let lastProgressTime = Date.now();
    let progressBuffer = '';
    let finished = false;
    const stderrBuffer = createFfmpegStderrBuffer(50);

    function cleanup() {
      clearInterval(stallCheck);
      if (hardTimeout) clearTimeout(hardTimeout);

      fs.promises.rm(overlayPath, { force: true }).catch((error) => {
        logger.warn('Video watermark overlay cleanup failed', {
          jobId: options?.jobId,
          overlayPath,
          error,
        });
      });
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
      progressBuffer += str;

      const lines = progressBuffer.split('\n');
      progressBuffer = lines.pop() || '';

      for (const line of lines) {
        if (!line.startsWith('out_time_ms=')) {
          stderrBuffer.push(line);
        } else {
          lastProgressTime = Date.now();

          if (onProgress) {
            const value = Number(line.split('=')[1]);

            if (!Number.isNaN(value)) {
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

    const stallCheck = setInterval(() => {
      const stallLimit = Number(process.env.FFMPEG_STALL_LIMIT_MS || 30 * 60 * 1000);

      if (stallLimit > 0 && Date.now() - lastProgressTime > stallLimit) {
        const err = new Error('FFmpeg stalled');
        logger.error('FFmpeg stalled', {
          jobId: options?.jobId,
          input,
          output,
          lastStderr: stderrBuffer.getLines(),
        });
        ffmpeg.kill('SIGKILL');
        safeReject(err);
      }
    }, 60_000);

    const maxRuntimeMs = Number(process.env.FFMPEG_MAX_RUNTIME_MS || 0);

    const hardTimeout =
      maxRuntimeMs > 0
        ? setTimeout(() => {
            const err = new Error('FFmpeg max runtime exceeded');
            logger.error('FFmpeg max runtime exceeded', {
              jobId: options?.jobId,
              input,
              output,
              maxRuntimeMs,
              lastStderr: stderrBuffer.getLines(),
            });
            ffmpeg.kill('SIGKILL');
            safeReject(err);
          }, maxRuntimeMs)
        : null;

    ffmpeg.on('close', (code) => {
      if (code === 0) {
        safeResolve();
      } else {
        logger.error('FFmpeg failed', {
          jobId: options?.jobId,
          exitCode: code,
          input,
          output,
          lastStderr: stderrBuffer.getLines(),
        });
        safeReject(new Error(`FFmpeg failed with code ${code}`));
      }
    });

    ffmpeg.on('error', (err) => {
      logger.error('FFmpeg spawn error', {
        jobId: options?.jobId,
        error: err,
        input,
        output,
        lastStderr: stderrBuffer.getLines(),
      });
      safeReject(err);
    });
  });
}

export function getDuration(file: string): number {
  const probe = inspectVideoInput(file);
  return probe.durationMs ?? 0;
}