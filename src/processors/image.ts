import sharp from 'sharp';
import { config } from '../config';
import {
  createRepeatedWatermarkOverlay,
  getDefaultWatermarkText,
} from '../utils/watermark';
import path from 'path/win32';

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
    compress?: boolean;
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
  }).rotate();

  if (options?.compress) {
    image.resize({
      width: targetWidth,
      withoutEnlargement: true,
    });
  }

  const overlay = await createRepeatedWatermarkOverlay({
    width: options?.compress ? targetWidth : (metadata.width || targetWidth),
    height: options?.compress ? targetHeight : (metadata.height || targetHeight),
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

  const outputPath = `${outputBase}${path.extname(input) || ".png"}`;

  if (options?.compress) {
    const plan = buildImageOutputPlan(metadata);
    await plan.apply(pipeline).toFile(outputPath);

    return {
      ext: plan.ext,
      outputPath: `${outputBase}${plan.ext}`,
    };
  }

  const originalExt =
    path.extname(input) ||
    (metadata.format ? `.${metadata.format}` : ".png");

  const finalOutput = `${outputBase}${originalExt}`;

  await pipeline.toFile(finalOutput);

  return {
    ext: originalExt,
    outputPath: finalOutput,
  };
}