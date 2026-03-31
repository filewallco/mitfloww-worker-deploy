import fs from 'fs';
import https from 'https';

const MODE = process.env.MODE || 'local';

// ALWAYS export download
export async function download(url: string, dest: string) {
  if (MODE === 'local' && url.startsWith('file://')) {
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

// ALWAYS export upload
export async function upload(filePath: string, key: string) {
  if (MODE === 'local') {
    const outputDir = './outputs';

    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir);
    }

    const fileName = key.split('/').pop();
    const dest = `${outputDir}/${fileName}`;

    await fs.promises.copyFile(filePath, dest);

    console.log(`Saved locally: ${dest}`);
    return;
  }

  // later replace with R2 SDK
  console.log(`Uploading ${filePath} to ${key}`);
}