import fs from 'fs';
import os from 'os';
import path from 'path';
import sharp from 'sharp';

export type WatermarkOverlayOptions = {
  width: number;
  height: number;
  text?: string;
  opacity?: number;
  angle?: number;
  density?: 'light' | 'normal' | 'dense';
};

function clamp(value: number, min: number, max: number) {
  if (!Number.isFinite(value)) return min;
  return Math.max(min, Math.min(max, value));
}

function escapeXml(value: string) {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function boolEnv(name: string, fallback = false) {
  const raw = process.env[name];

  if (raw == null || raw === '') return fallback;

  return ['1', 'true', 'yes', 'on'].includes(raw.toLowerCase());
}

function resolveWatermarkLogoPath() {
  const configured = process.env.WATERMARK_LOGO_PATH?.trim();

  if (configured) {
    return path.isAbsolute(configured)
      ? configured
      : path.resolve(process.cwd(), configured);
  }

  return path.resolve(process.cwd(), 'assets', 'watermark.png');
}

function readLogoBase64() {
  const logoPath = resolveWatermarkLogoPath();

  if (!fs.existsSync(logoPath)) {
    return null;
  }

  return fs.readFileSync(logoPath).toString('base64');
}

export function isLogoWatermarkEnabled() {
  return boolEnv('IS_LOGO_WATERMARK', false);
}

export function getDefaultWatermarkText() {
  return process.env.WATERMARK_TEXT?.trim() || 'MITFLOWW';
}

export function buildTraceableWatermarkText(input?: {
  userEmail?: string | null;
  userName?: string | null;
  fileVersionId?: string | null;
}) {
  const brand = getDefaultWatermarkText();

  const user =
    input?.userEmail?.trim() ||
    input?.userName?.trim() ||
    '';

  const version = input?.fileVersionId
    ? `FV-${input.fileVersionId.replace(/-/g, '').slice(0, 6).toUpperCase()}`
    : '';

  return [brand, user, version].filter(Boolean).join(' · ');
}

function getPatternSpacing(input: {
  baseSize: number;
  density: 'light' | 'normal' | 'dense';
  mode: 'text' | 'logo';
}) {
  const { baseSize, density, mode } = input;

  if (mode === 'logo') {
    return {
      xGap:
        density === 'dense'
          ? baseSize * 3
          : density === 'light'
            ? baseSize * 5
            : baseSize * 4,

      yGap:
        density === 'dense'
          ? baseSize * 2
          : density === 'light'
            ? baseSize * 3.5
            : baseSize * 2.8,
    };
  }

  return {
    xGap:
      density === 'dense'
        ? baseSize * 7
        : density === 'light'
          ? baseSize * 13
          : baseSize * 10,
    yGap:
      density === 'dense'
        ? baseSize * 4.5
        : density === 'light'
          ? baseSize * 7.5
          : baseSize * 6,
  };
}

function createRepeatedTextNodes(input: {
  text: string;
  startX: number;
  endX: number;
  startY: number;
  endY: number;
  xGap: number;
  yGap: number;
}) {
  const nodes: string[] = [];

  for (let y = input.startY; y <= input.endY; y += input.yGap) {
    const rowOffset =
      Math.floor((y - input.startY) / input.yGap) % 2 === 0
        ? 0
        : input.xGap / 2;

    for (let x = input.startX - rowOffset; x <= input.endX; x += input.xGap) {
      nodes.push(`
        <text
          x="${Math.round(x)}"
          y="${Math.round(y)}"
          text-anchor="middle"
          dominant-baseline="middle"
        >${input.text}</text>
      `);
    }
  }

  return nodes.join('\n');
}

function createRepeatedLogoNodes(input: {
  logoBase64: string;
  startX: number;
  endX: number;
  startY: number;
  endY: number;
  xGap: number;
  yGap: number;
  logoWidth: number;
  logoHeight: number;
  logoOpacity: number;
}) {
  const nodes: string[] = [];

  const stripHeight = input.logoHeight * 1.8;

  for (let y = input.startY; y <= input.endY; y += input.yGap) {
    const rowOffset =
      Math.floor((y - input.startY) / input.yGap) % 2 === 0
        ? 0
        : input.xGap / 2;

    /*
     * One continuous translucent strip per row
     */
    nodes.push(`
      <rect
        x="${Math.round(input.startX)}"
        y="${Math.round(y - stripHeight / 2)}"
        width="${Math.round(input.endX - input.startX)}"
        height="${Math.round(stripHeight)}"
        rx="${Math.round(stripHeight / 2)}"
        fill="#000000"
        fill-opacity="0.22"
      />
    `);

    /*
     * Logos on top of the strip
     */
    for (
      let x = input.startX - rowOffset;
      x <= input.endX;
      x += input.xGap
    ) {
      const imageX = Math.round(x - input.logoWidth / 2);
      const imageY = Math.round(y - input.logoHeight / 2);

      nodes.push(`
        <image
          href="data:image/png;base64,${input.logoBase64}"
          x="${imageX}"
          y="${imageY}"
          width="${Math.round(input.logoWidth)}"
          height="${Math.round(input.logoHeight)}"
          opacity="${input.logoOpacity}"
          preserveAspectRatio="xMidYMid meet"
        />
      `);
    }
  }

  return nodes.join('\n');
}

/**
 * Creates a transparent watermark overlay.
 *
 * IS_LOGO_WATERMARK=true:
 *   repeated PNG only
 *
 * IS_LOGO_WATERMARK=false:
 *   repeated text only
 */
export function createRepeatedWatermarkSvg(options: WatermarkOverlayOptions) {
  const width = Math.max(1, Math.round(options.width));
  const height = Math.max(1, Math.round(options.height));

  const shorterSide = Math.min(width, height);
  const density = options.density || 'normal';
  const angle = options.angle ?? Number(process.env.WATERMARK_ANGLE || -32);

  const opacity = clamp(
    options.opacity ?? Number(process.env.WATERMARK_OPACITY || 0.12),
    0.035,
    0.22,
  );

  const logoOpacity = clamp(
    Number(process.env.WATERMARK_LOGO_OPACITY || 0.62),
    0.15,
    0.75,
  );

  const spread = Math.ceil(Math.sqrt(width * width + height * height));
  const startX = -spread;
  const endX = width + spread;
  const startY = -spread;
  const endY = height + spread;

  const logoBase64 = isLogoWatermarkEnabled() ? readLogoBase64() : null;

  if (logoBase64) {
    const logoWidth = clamp(
      Math.round(shorterSide * Number(process.env.WATERMARK_LOGO_WIDTH_RATIO || 0.18)),
      52,
      180,
    );

    const logoHeight = clamp(
      Math.round(logoWidth * Number(process.env.WATERMARK_LOGO_HEIGHT_RATIO || 0.32)),
      24,
      90,
    );

    const { xGap, yGap } = getPatternSpacing({
      baseSize: logoWidth,
      density,
      mode: 'logo',
    });

    const contentNodes = createRepeatedLogoNodes({
      logoBase64,
      startX,
      endX,
      startY,
      endY,
      xGap,
      yGap,
      logoWidth,
      logoHeight,
      logoOpacity,
    });

    return Buffer.from(`
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="${width}"
        height="${height}"
        viewBox="0 0 ${width} ${height}"
      >
        <rect width="100%" height="100%" fill="transparent"/>
        <g
          transform="rotate(${angle} ${width / 2} ${height / 2})"
        >
          ${contentNodes}
        </g>
      </svg>
    `);
  }

  const fontSize = clamp(Math.round(shorterSide * 0.042), 16, 52);
  const text = escapeXml(options.text?.trim() || getDefaultWatermarkText());

  const { xGap, yGap } = getPatternSpacing({
    baseSize: fontSize,
    density,
    mode: 'text',
  });

  const contentNodes = createRepeatedTextNodes({
    text,
    startX,
    endX,
    startY,
    endY,
    xGap,
    yGap,
  });

  return Buffer.from(`
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="${width}"
      height="${height}"
      viewBox="0 0 ${width} ${height}"
    >
      <rect width="100%" height="100%" fill="transparent"/>
      <g
        transform="rotate(${angle} ${width / 2} ${height / 2})"
        opacity="${opacity}"
        font-family="Arial, Helvetica, sans-serif"
        font-size="${fontSize}"
        font-weight="700"
        letter-spacing="${Math.max(1, Math.round(fontSize * 0.05))}"
        fill="#ffffff"
        stroke="#000000"
        stroke-width="${Math.max(0.5, fontSize * 0.03)}"
        paint-order="stroke fill"
      >
        ${contentNodes}
      </g>
    </svg>
  `);
}

export async function createRepeatedWatermarkOverlay(options: WatermarkOverlayOptions) {
  const svg = createRepeatedWatermarkSvg(options);

  return sharp(svg, {
    limitInputPixels: false,
  })
    .png()
    .toBuffer();
}

export async function createRepeatedWatermarkOverlayFile(
  jobId: string,
  options: WatermarkOverlayOptions,
) {
  const overlay = await createRepeatedWatermarkOverlay(options);

  const safeJobId = jobId.replace(/[^a-zA-Z0-9_-]/g, '');
  const filePath = path.join(
    os.tmpdir(),
    `mitfloww-watermark-${safeJobId}-${Date.now()}.png`,
  );

  await fs.promises.writeFile(filePath, overlay);

  return filePath;
}