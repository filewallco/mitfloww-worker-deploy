import sharp from 'sharp';
import path from 'path';

export async function processImage(
  input: string,
  outputBase: string
): Promise<{ ext: string; outputPath: string }> {

  const watermarkPath = path.resolve(
    __dirname,
    '../../assets/watermark.png'
  );
  
  const image = sharp(input);
  const metadata = await image.metadata();

  const resized = image.resize({
    width: Math.min(metadata.width || 1024, 1024),
    withoutEnlargement: true,
  });

  const wmWidth = Math.floor((metadata.width || 1000) * 0.1);
  const watermark = await sharp(watermarkPath)
    .resize(wmWidth)
    .png()
    .toBuffer();

  const pipeline = resized.composite([
    {
      input: watermark,
      gravity: 'southeast',
    },
  ]);

  // Detect actual format (NOT extension)
  const format = metadata.format || 'jpeg';

  const ext = format === 'png' ? '.png' : '.jpg';

  const outputPath = `${outputBase}${ext}`;

  await pipeline.toFile(outputPath);

  return { ext, outputPath };
}