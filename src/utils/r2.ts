import fs from 'fs';
import https from 'https';
import path from 'path';

const MODE = process.env.MODE || 'local';

export async function download(url: string, dest: string) {
  if (MODE === 'local' && url.startsWith('file://')) {
    const localPath = url.replace('file://', '');
    await fs.promises.copyFile(localPath, dest);
    return;
  }

  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);

    const req = https.get(url, (res) => {
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode}`));
      }

      res.pipe(file);

      file.on('finish', () => {
        file.close();
        resolve(true);
      });
    });

    req.setTimeout(30000, () => {
      req.destroy();
      reject(new Error('Download timeout'));
    });

    req.on('error', reject);
  });
}

export async function upload(filePath: string, key: string): Promise<string> {
  if (MODE === 'local') {
    const outputDir = './outputs';

    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir);
    }

    const dest = path.join(outputDir, key);
    await fs.promises.mkdir(path.dirname(dest), { recursive: true });
    await fs.promises.copyFile(filePath, dest);

    console.log(`Saved locally: ${dest}`);

    return dest; // RETURN PATH
  }

  // TODO: Replace with real R2 upload
  const url = `https://your-r2-domain/${key}`;

  console.log(`Uploading ${filePath} to ${url}`);

  return url; // RETURN URL
}