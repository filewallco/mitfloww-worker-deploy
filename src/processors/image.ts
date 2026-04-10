import sharp from 'sharp';
import path from 'path';

type ImageOutputPlan = {
  ext: string;
  apply: (pipeline: sharp.Sharp) => sharp.Sharp;
};

function buildImageOutputPlan(metadata: sharp.Metadata): ImageOutputPlan {
  const format = (metadata.format || '').toLowerCase();
  const hasAlpha = Boolean(metadata.hasAlpha);
  const isAnimated = (metadata.pages || 1) > 1;

  if (isAnimated) {
    if (format === 'gif') {
      return {
        ext: '.gif',
        apply: (pipeline) =>
          pipeline.gif({
            effort: 3,
            reuse: true,
          }),
      };
    }

    return {
      ext: '.webp',
      apply: (pipeline) =>
        pipeline.webp({
          quality: 80,
          effort: 4,
        }),
    };
  }

  if (format === 'webp') {
    return {
      ext: '.webp',
      apply: (pipeline) =>
        pipeline.webp({
          quality: hasAlpha ? 90 : 82,
          effort: 4,
        }),
    };
  }

  if (format === 'png' || format === 'svg' || hasAlpha) {
    return {
      ext: '.png',
      apply: (pipeline) =>
        pipeline.png({
          compressionLevel: 9,
          palette: !hasAlpha,
        }),
    };
  }

  return {
    ext: '.jpg',
    apply: (pipeline) =>
      pipeline.jpeg({
        quality: 82,
        mozjpeg: true,
      }),
  };
}

export async function processImage(
  input: string,
  outputBase: string
): Promise<{ ext: string; outputPath: string }> {
  const watermarkPath = path.resolve(
    __dirname,
    '../../assets/watermark.png'
  );

  const probe = sharp(input, { animated: true });
  const metadata = await probe.metadata();

  const image = sharp(input, {
    animated: (metadata.pages || 1) > 1,
  }).rotate();

  const targetWidth = Math.min(metadata.width || 1024, 1024);
  const resized = image.resize({
    width: targetWidth,
    withoutEnlargement: true,
  });

  const wmWidth = Math.max(48, Math.floor(targetWidth * 0.1));
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

  const plan = buildImageOutputPlan(metadata);
  const ext = plan.ext;
  const outputPath = `${outputBase}${ext}`;

  await plan.apply(pipeline).toFile(outputPath);

  return { ext, outputPath };
}
