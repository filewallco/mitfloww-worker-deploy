import fs from 'fs';
import path from 'path';
import { PDFDocument, ParseSpeeds } from 'pdf-lib';
import { config } from '../config';

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

async function applyPdfWatermark(input: string, outputPath: string): Promise<void> {
  const stat = await fs.promises.stat(input);

  if (!stat.isFile() || stat.size > config.security.maxPdfBytes) {
    throw new Error('PDF exceeds safety size limit');
  }

  const deadline = Date.now() + config.security.pdfProcessingTimeoutMs;
  const watermarkPath = path.resolve(__dirname, '../../assets/watermark.png');
  const [pdfBytes, watermarkBytes] = await Promise.all([
    fs.promises.readFile(input),
    fs.promises.readFile(watermarkPath),
  ]);

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

  const watermark = await pdfDoc.embedPng(watermarkBytes);

  for (const page of pages) {
    assertDeadline(deadline);

    const { width: pageWidth, height: pageHeight } = page.getSize();
    assertPageSize(pageWidth, pageHeight);

    const margin = Math.max(18, Math.min(pageWidth, pageHeight) * 0.035);
    const maxWatermarkWidth = Math.min(180, pageWidth * 0.22);
    const maxWatermarkHeight = pageHeight * 0.14;
    const watermarkSize = watermark.scaleToFit(maxWatermarkWidth, maxWatermarkHeight);

    page.drawImage(watermark, {
      x: pageWidth - watermarkSize.width - margin,
      y: margin,
      width: watermarkSize.width,
      height: watermarkSize.height,
      opacity: 0.35,
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

export async function processPdf(input: string, outputBase: string): Promise<PdfOutput> {
  const ext = '.pdf';
  const outputPath = `${outputBase}${ext}`;

  await withTimeout(
    applyPdfWatermark(input, outputPath),
    config.security.pdfProcessingTimeoutMs
  );

  return { ext, outputPath };
}
