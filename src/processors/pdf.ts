import fs from 'fs';
import {
  degrees,
  PDFDocument,
  ParseSpeeds,
  PDFImage,
  rgb,
  StandardFonts,
} from 'pdf-lib';
import { config } from '../config';
import {
  createRepeatedWatermarkOverlay,
  getDefaultWatermarkText,
  isLogoWatermarkEnabled,
} from '../utils/watermark';

type PdfOutput = {
  ext: string;
  outputPath: string;
};

function withTimeout<T>(task: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error('PDF processing timeout'));
    }, timeoutMs);

    task.then(resolve, reject).finally(() => clearTimeout(timer));
  });
}

function assertDeadline(deadline: number) {
  if (Date.now() > deadline) {
    throw new Error('PDF processing timeout');
  }
}

function assertPageSize(width: number, height: number) {
  const maxDimension = config.security.maxPdfPageDimension;

  if (
    !Number.isFinite(width) ||
    !Number.isFinite(height) ||
    width <= 0 ||
    height <= 0 ||
    width > maxDimension ||
    height > maxDimension
  ) {
    throw new Error('PDF page dimensions exceed safety limit');
  }
}

function fitTextFontSize(input: {
  text: string;
  pageWidth: number;
  pageHeight: number;
}) {
  const shorterSide = Math.min(input.pageWidth, input.pageHeight);
  const longerSide = Math.max(input.pageWidth, input.pageHeight);

  const target = longerSide * 0.78;
  const estimatedCharWidth = Math.max(1, input.text.length * 0.58);

  return Math.max(
    36,
    Math.min(shorterSide * 0.16, target / estimatedCharWidth),
  );
}

const yieldToEventLoop = () =>
  new Promise<void>((resolve) => setImmediate(resolve));

async function applyPdfWatermark(
  input: string,
  outputPath: string,
  options?: {
    watermarkText?: string;
  },
): Promise<void> {
  const stat = await fs.promises.stat(input);

  if (!stat.isFile() || stat.size > config.security.maxPdfBytes) {
    throw new Error('PDF exceeds safety size limit');
  }

  const deadline = Date.now() + config.security.pdfProcessingTimeoutMs;

  let pdfBytes = await fs.promises.readFile(input);

  assertDeadline(deadline);

  let pdfDoc: PDFDocument;

  try {
    pdfDoc = await PDFDocument.load(pdfBytes, {
      ignoreEncryption: false,
      parseSpeed: ParseSpeeds.Fast,
      throwOnInvalidObject: true,
      updateMetadata: false,
      capNumbers: true,
    });
  } catch (err: any) {
    if (/encrypt/i.test(err?.message || '')) {
      throw new Error('Encrypted PDFs are not supported');
    }

    throw new Error('Malformed PDF rejected');
  }

  // Release the original file buffer as soon as possible.
  pdfBytes = Buffer.alloc(0);

  const pages = pdfDoc.getPages();

  if (pages.length === 0 || pages.length > config.security.maxPdfPages) {
    throw new Error('PDF page count exceeds safety limit');
  }

  const font = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  const text = options?.watermarkText || getDefaultWatermarkText();

  const opacity = Math.max(
    0.06,
    Math.min(Number(process.env.PDF_WATERMARK_OPACITY || 0.14), 0.25),
  );

  const logoWatermarkEnabled = isLogoWatermarkEnabled();

  // Cache embedded overlays by page size
  const overlayCache = new Map<string, PDFImage>();

  for (const page of pages) {
    assertDeadline(deadline);

    const { width: pageWidth, height: pageHeight } = page.getSize();

    assertPageSize(pageWidth, pageHeight);

    if (logoWatermarkEnabled) {
      const roundedWidth = Math.round(pageWidth);
      const roundedHeight = Math.round(pageHeight);

      const cacheKey =
        `${roundedWidth}:${roundedHeight}:${text}:${opacity}:light`;

      let overlayImage = overlayCache.get(cacheKey);

      if (!overlayImage) {
        const overlay = await createRepeatedWatermarkOverlay({
          width: roundedWidth,
          height: roundedHeight,
          text,
          opacity,
          density: 'light',
        });

        overlayImage = await pdfDoc.embedPng(overlay);
        overlayCache.set(cacheKey, overlayImage);
      }

      page.drawImage(overlayImage, {
        x: 0,
        y: 0,
        width: pageWidth,
        height: pageHeight,
      });

      await yieldToEventLoop();
      continue;
    }

    const fontSize = fitTextFontSize({
      text,
      pageWidth,
      pageHeight,
    });

    const textWidth = font.widthOfTextAtSize(text, fontSize);

    page.drawText(text, {
      x: pageWidth / 2 - textWidth / 2,
      y: pageHeight / 2,
      size: fontSize,
      font,
      color: rgb(0.15, 0.15, 0.15),
      opacity,
      rotate: degrees(-32),
    });

    await yieldToEventLoop();
  }

  const outputBytes = await pdfDoc.save({
    useObjectStreams: true,
    addDefaultPage: false,
    objectsPerTick: 50,
  });

  overlayCache.clear();

  if (outputBytes.byteLength > config.security.maxOutputBytes) {
    throw new Error('PDF output exceeds safety size limit');
  }

  await fs.promises.writeFile(outputPath, outputBytes);

  // Drop references for earlier GC in long-running workers
  (pdfDoc as any) = null;
}

export async function processPdf(
  input: string,
  outputBase: string,
  options?: {
    watermarkText?: string;
  },
): Promise<PdfOutput> {
  const ext = '.pdf';
  const outputPath = `${outputBase}${ext}`;

  await withTimeout(
    applyPdfWatermark(input, outputPath, options),
    config.security.pdfProcessingTimeoutMs,
  );

  return { ext, outputPath };
}