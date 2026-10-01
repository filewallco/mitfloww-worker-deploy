import { execFileSync, spawn } from 'child_process';
import fs from 'fs';
import os from 'os';
import { config } from '../config';
import { createFfmpegStderrBuffer, logger } from '../utils/logger';
import { resolveWatermarkAudioPath } from '../utils/watermark';

type FfprobeStream = {
  codec_type?: string;
  codec_name?: string;
  channels?: number;
  sample_rate?: string;
  duration?: string;
};

type FfprobeFormat = {
  format_name?: string;
  duration?: string;
};

type FfprobePayload = {
  streams?: FfprobeStream[];
  format?: FfprobeFormat;
};

export type AudioProbe = {
  formatName: string | null;
  durationMs: number | null;
  hasAudio: boolean;
  audioCodec: string | null;
  channels: number | null;
  sampleRate: number | null;
};

const ALLOWED_AUDIO_CODECS = new Set([
  'aac',
  'mp3',
  'opus',
  'vorbis',
  'pcm_s16le',
  'pcm_s24le',
  'pcm_s32le',
  'pcm_f32le',
  'flac',
  'alac',
  'ac3',
  'eac3',
]);

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
    'format=format_name,duration:stream=codec_type,codec_name,channels,sample_rate,duration',
    '-of',
    'json',
    ...FFPROBE_PROTOCOL_ARGS,
    file,
  ]);

  return JSON.parse(out.toString()) as FfprobePayload;
}

export function inspectAudioInput(file: string): AudioProbe {
  const meta = runFfprobe(file);
  const audioStream = meta.streams?.find((s) => s.codec_type === 'audio');
  const durationSec = meta.format?.duration
    ? Number(meta.format.duration)
    : audioStream?.duration
      ? Number(audioStream.duration)
      : null;
  const durationMs =
    durationSec != null && Number.isFinite(durationSec)
      ? Math.max(0, Math.round(durationSec * 1000))
      : null;

  return {
    formatName: meta.format?.format_name ?? null,
    durationMs,
    hasAudio: Boolean(audioStream),
    audioCodec: audioStream?.codec_name ?? null,
    channels: audioStream?.channels ?? null,
    sampleRate: audioStream?.sample_rate ? Number(audioStream.sample_rate) : null,
  };
}

export function assertSafeAudio(file: string): AudioProbe {
  const probe = inspectAudioInput(file);

  if (!probe.hasAudio) {
    throw new Error('No audio stream found');
  }

  if (probe.audioCodec && !ALLOWED_AUDIO_CODECS.has(probe.audioCodec)) {
    throw new Error(`Unsupported audio codec: ${probe.audioCodec}`);
  }

  return probe;
}

export type AudioProcessOptions = {
  jobId?: string;
  watermarkAudioPath?: string;
};

function clampProgress(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(value, 100));
}

export async function processAudio(
  input: string,
  outputBase: string,
  options?: AudioProcessOptions,
  onProgress?: (progress: number) => void
): Promise<{ outputPath: string; ext: string }> {
  assertSafeAudio(input);

  const probe = inspectAudioInput(input);
  const totalDurationMs = probe.durationMs ?? 10000;
  const durationSec = Math.max(0.1, totalDurationMs / 1000);

  const watermarkAudioPath = options?.watermarkAudioPath ?? resolveWatermarkAudioPath();
  if (!fs.existsSync(watermarkAudioPath)) {
    throw new Error(`Watermark audio file not found: ${watermarkAudioPath}`);
  }

  const finalOutput = `${outputBase}.mp3`;
  const midSec = Math.max(0.05, durationSec / 2);

  // Audio filter: split original into [p1] and [p2], insert watermark [w1] in between, then concat
  const filterComplex =
    `[0:a]atrim=0:${midSec},asetpts=PTS-STARTPTS[p1];` +
    `[0:a]atrim=${midSec}:${durationSec},asetpts=PTS-STARTPTS[p2];` +
    `[1:a]asetpts=PTS-STARTPTS[w1];` +
    `[p1][w1][p2]concat=n=3:v=0:a=1[outa]`;

  const args = [
    '-y',
    ...LOCAL_INPUT_ARGS,
    '-i',
    input,
    ...LOCAL_INPUT_ARGS,
    '-i',
    watermarkAudioPath,
    '-filter_complex',
    filterComplex,
    '-map',
    '[outa]',
    '-c:a',
    'libmp3lame',
    '-b:a',
    '192k',
    '-progress',
    'pipe:2',
    finalOutput,
  ];

  await new Promise<void>((resolve, reject) => {
    const ffmpeg = spawn(config.ffmpegPath, args, {
      stdio: ['ignore', 'ignore', 'pipe'],
    });

    let lastProgressTime = Date.now();
    let progressBuffer = '';
    let finished = false;
    const stderrBuffer = createFfmpegStderrBuffer(50);

    const stallCheck = setInterval(() => {
      const stallLimit = Number(process.env.FFMPEG_STALL_LIMIT_MS || 30 * 60 * 1000);
      if (stallLimit > 0 && Date.now() - lastProgressTime > stallLimit) {
        clearInterval(stallCheck);
        ffmpeg.kill('SIGKILL');
        safeReject(new Error('FFmpeg audio watermarking stalled'));
      }
    }, 30_000);

    function cleanup() {
      clearInterval(stallCheck);
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

          if (onProgress && totalDurationMs > 0) {
            const value = Number(line.split('=')[1]);
            if (!Number.isNaN(value)) {
              const processedMs = value / 1000;
              const scaled = (processedMs / (totalDurationMs + 1500)) * 100;
              onProgress(clampProgress(scaled));
            }
          }
        }
      }
    });

    ffmpeg.on('close', (code) => {
      if (code === 0) {
        safeResolve();
      } else {
        logger.error('FFmpeg audio watermarking failed', {
          jobId: options?.jobId,
          exitCode: code,
          input,
          output: finalOutput,
          lastStderr: stderrBuffer.getLines(),
        });
        safeReject(new Error(`FFmpeg failed with code ${code}`));
      }
    });

    ffmpeg.on('error', (err) => {
      logger.error('FFmpeg spawn error on audio', {
        jobId: options?.jobId,
        error: err,
        input,
        output: finalOutput,
        lastStderr: stderrBuffer.getLines(),
      });
      safeReject(err);
    });
  });

  return {
    outputPath: finalOutput,
    ext: '.mp3',
  };
}
