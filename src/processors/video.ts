import { execFile } from 'child_process';

export function processVideo(input: string, output: string): Promise<void> {
  return new Promise((resolve, reject) => {
    execFile(
      'ffmpeg',
      [
        '-i', input,
        '-i', 'assets/watermark.png',

        // scale + overlay watermark
        '-filter_complex',
        'scale=-2:360[video];[video][1]overlay=10:10',

        '-c:v', 'libx264',
        '-crf', '28',
        '-preset', 'fast',

        '-c:a', 'copy',

        output,
      ],
      (err, stdout, stderr) => {
        if (err) {
          console.error(stderr);
          return reject(err);
        }
        resolve();
      }
    );
  });
}