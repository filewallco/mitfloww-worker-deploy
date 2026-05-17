import sharp from 'sharp';
import { config } from '../config';
import {
  createRepeatedWatermarkOverlay,
  getDefaultWatermarkText,
} from '../utils/watermark';

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
        quality: 84,
        mozjpeg: true,
      }),
  };
}

function getResizedDimensions(metadata: sharp.Metadata) {
  const originalWidth = metadata.width || 1024;
  const originalHeight = metadata.height || 1024;

  const targetWidth = Math.min(originalWidth, 1600);
  const targetHeight = Math.max(
    1,
    Math.round((originalHeight * targetWidth) / originalWidth),
  );

  return { targetWidth, targetHeight };
}

export async function processImage(
  input: string,
  outputBase: string,
  options?: {
    watermarkText?: string;
  },
): Promise<{ ext: string; outputPath: string }> {
  const probe = sharp(input, {
    animated: true,
    limitInputPixels: config.security.maxImagePixels,
  });

  const metadata = await probe.metadata();
  const totalPixels =
    (metadata.width || 0) *
    (metadata.height || 0) *
    (metadata.pages || 1);

  if (totalPixels > config.security.maxImagePixels) {
    throw new Error('Image exceeds pixel limit');
  }

  const { targetWidth, targetHeight } = getResizedDimensions(metadata);

  const image = sharp(input, {
    animated: (metadata.pages || 1) > 1,
    limitInputPixels: config.security.maxImagePixels,
  })
    .rotate()
    .resize({
      width: targetWidth,
      withoutEnlargement: true,
    });

  const overlay = await createRepeatedWatermarkOverlay({
    width: targetWidth,
    height: targetHeight,
    text: options?.watermarkText || getDefaultWatermarkText(),
    opacity: Number(process.env.IMAGE_WATERMARK_OPACITY || 0.16),
    density: 'normal',
  });

  const pipeline = image.composite([
    {
      input: overlay,
      left: 0,
      top: 0,
      blend: 'over',
    },
  ]);

  const plan = buildImageOutputPlan(metadata);
  const ext = plan.ext;
  const outputPath = `${outputBase}${ext}`;

  await plan.apply(pipeline).toFile(outputPath);

  return { ext, outputPath };
}