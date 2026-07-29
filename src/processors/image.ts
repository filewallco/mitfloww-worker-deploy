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
  // Reduce retained memory between jobs
  sharp.cache(false);

  // Lower peak memory usage (may reduce throughput slightly)
  sharp.concurrency(1);

  const compress = options?.compress === true;

  const probe = sharp(input, {
    animated: true,
    limitInputPixels: config.security.maxImagePixels,
  });

  const metadata = await probe.metadata();

  const animated = (metadata.pages || 1) > 1;

  const totalPixels =
    (metadata.width || 0) *
    (metadata.height || 0) *
    (metadata.pages || 1);

  if (totalPixels > config.security.maxImagePixels) {
    throw new Error('Image exceeds pixel limit');
  }

  const { targetWidth, targetHeight } = getResizedDimensions(metadata);

  const image = sharp(input, {
    animated,
    limitInputPixels: config.security.maxImagePixels,
  }).rotate();

  if (compress) {
    image.resize({
      width: targetWidth,
      withoutEnlargement: true,
    });
  }

  const watermarkWidth = compress
    ? targetWidth
    : (metadata.width || targetWidth);

  const watermarkHeight = compress
    ? targetHeight
    : (metadata.height || targetHeight);

  const watermarkText =
    options?.watermarkText || getDefaultWatermarkText();

  const opacity = Number(
    process.env.IMAGE_WATERMARK_OPACITY || 0.16,
  );

  const overlay = await createRepeatedWatermarkOverlay({
    width: watermarkWidth,
    height: watermarkHeight,
    text: watermarkText,
    opacity,
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

  const inputExtension =
    path.extname(input) ||
    (metadata.format ? `.${metadata.format}` : '.png');

  if (compress) {
    const plan = buildImageOutputPlan(metadata);

    const finalOutput = `${outputBase}${plan.ext}`;

    await plan.apply(pipeline).toFile(finalOutput);

    return {
      ext: plan.ext,
      outputPath: finalOutput,
    };
  }

  const finalOutput = `${outputBase}${inputExtension}`;

  await pipeline.toFile(finalOutput);

  return {
    ext: inputExtension,
    outputPath: finalOutput,
  };
}