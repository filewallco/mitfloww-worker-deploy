import fs from 'fs';
import path from 'path';

const LOG_DIR = path.resolve(process.cwd(), 'logs');

type Meta = Record<string, any> | undefined;

function ensureLogDir() {
  try {
    fs.mkdirSync(LOG_DIR, { recursive: true });
  } catch {}
}

function safeSerialize(obj: any): string {
  try {
    const seen = new WeakSet();

    return JSON.stringify(obj, function (_key, value) {
      if (value instanceof Error) {
        return {
          name: value.name,
          message: value.message,
          stack: value.stack,
        };
      }

      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) return '[Circular]';
        seen.add(value);
      }

      return value;
    });
  } catch (e) {
    try {
      return String(obj);
    } catch {
      return '[Unserializable]';
    }
  }
}

class Logger {
  private stream: fs.WriteStream | null = null;
  private currentDate: string | null = null;

  constructor() {
    ensureLogDir();
  }

  private rotateIfNeeded() {
    const date = new Date().toISOString().slice(0, 10);
    if (this.currentDate === date && this.stream) return;

    try {
      this.stream?.end();
    } catch {}

    this.currentDate = date;
    const file = path.join(LOG_DIR, `${date}.log`);
    try {
      this.stream = fs.createWriteStream(file, { flags: 'a' });
    } catch (e) {
      this.stream = null;
    }
  }

  private write(level: string, message: string, meta?: Meta) {
    try {
      this.rotateIfNeeded();

      const time = new Date().toISOString();
      const metaStr = meta ? ` ${safeSerialize(meta)}` : '';
      const line = `[${time}] [${level}] ${message}${metaStr}\n`;

      if (this.stream) {
        this.stream.write(line);
      } else {
        // Fallback to appendFile if stream couldn't be created
        fs.appendFile(path.join(LOG_DIR, `${this.currentDate || new Date().toISOString().slice(0,10)}.log`), line, () => {});
      }
    } catch {
      // Never throw from logger
    }
  }

  info(message: string, meta?: Meta) {
    this.write('INFO', message, meta);
  }

  warn(message: string, meta?: Meta) {
    this.write('WARN', message, meta);
  }

  error(message: string, meta?: Meta) {
    this.write('ERROR', message, meta);
  }

  fatal(message: string, meta?: Meta) {
    this.write('FATAL', message, meta);
  }
}

export const logger = new Logger();

/**
 * Rolling stderr buffer for FFmpeg.
 * Keeps the last N lines in memory for safe logging on failure.
 */
export function createFfmpegStderrBuffer(maxLines = 50) {
  const lines: string[] = [];

  return {
    push(chunk: string) {
      const parts = chunk.split(/\r?\n/);
      for (const p of parts) {
        if (!p) continue;
        lines.push(p);
        if (lines.length > maxLines) lines.shift();
      }
    },
    getLines() {
      return lines.slice();
    },
  };
}

export default logger;
