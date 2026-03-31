import fs from 'fs';
import https from 'https';

// for server hosting uncomment this
// export async function download(url: string, dest: string) {
//   return new Promise((resolve, reject) => {
//     const file = fs.createWriteStream(dest);
//     https.get(url, (res: { pipe: (arg0: any) => void; }) => {
//       res.pipe(file);
//       file.on('finish', () => file.close(resolve));
//     }).on('error', reject);
//   });
// }

// export async function upload(filePath: string, key: string) {
//   // Replace with real R2 SDK
//   console.log(`Uploading ${filePath} to ${key}`);
// }

// for local testing uncomment this
export async function download(url: string, dest: string) {
  if (url.startsWith('file://')) {
    const localPath = url.replace('file://', '');
    await fs.promises.copyFile(localPath, dest);
    return;
  }

  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    https.get(url, (res) => {
      res.pipe(file);
      file.on('finish', () => {
        file.close();
        resolve(true);
      });
    }).on('error', reject);
  });
}