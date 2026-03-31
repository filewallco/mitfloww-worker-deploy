import { execFile } from 'child_process';

export function processVideo(input: string, output: string): Promise<void> {
  return new Promise((resolve, reject) => {
    execFile(
      'ffmpeg',
      [
        '-i', input,
        '-vf', 'scale=-2:360',
        '-c:v', 'libx264',
        '-crf', '28',
        '-preset', 'fast',
        output,
      ],
      (err: any) => {
        if (err) return reject(err);
        resolve();
      }
    );
  });
}