import fs from 'fs';
import {
  degrees,
  PDFDocument,
  ParseSpeeds,
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
  const pdfBytes = await fs.promises.readFile(input);

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

  for (const page of pages) {
    assertDeadline(deadline);

    const { width: pageWidth, height: pageHeight } = page.getSize();
    assertPageSize(pageWidth, pageHeight);

        if (isLogoWatermarkEnabled()) {
      const overlay = await createRepeatedWatermarkOverlay({
        width: Math.round(pageWidth),
        height: Math.round(pageHeight),
        text,
        opacity,
        density: 'light',
      });

      const overlayImage = await pdfDoc.embedPng(overlay);

      page.drawImage(overlayImage, {
        x: 0,
        y: 0,
        width: pageWidth,
        height: pageHeight,
      });

      await new Promise((resolve) => setImmediate(resolve));
      continue;
    }

    const fontSize = fitTextFontSize({
      text,
      pageWidth,
      pageHeight,
    });

    const textWidth = font.widthOfTextAtSize(text, fontSize);
    const x = pageWidth / 2 - textWidth / 2;
    const y = pageHeight / 2;

    page.drawText(text, {
      x,
      y,
      size: fontSize,
      font,
      color: rgb(0.15, 0.15, 0.15),
      opacity,
      rotate: degrees(-32),
    });

    await new Promise((resolve) => setImmediate(resolve));
  }

  const outputBytes = await pdfDoc.save({
    useObjectStreams: true,
    addDefaultPage: false,
    objectsPerTick: 50,
  });

  if (outputBytes.byteLength > config.security.maxOutputBytes) {
    throw new Error('PDF output exceeds safety size limit');
  }

  await fs.promises.writeFile(outputPath, outputBytes);
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