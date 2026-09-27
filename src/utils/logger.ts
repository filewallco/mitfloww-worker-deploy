import fs from 'fs';
import path from 'path';
import { Pool } from 'pg';

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

// Optional database pool for persistent worker operational logs
let dbPool: Pool | null = null;
if (process.env.DATABASE_URL) {
  try {
    dbPool = new Pool({
      connectionString: process.env.DATABASE_URL,
      max: 3,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    });
    dbPool.on('error', () => {
      // Idle client error caught cleanly
    });
  } catch {
    dbPool = null;
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
      const metaStr = meta ? ` ${safeSerialize(meta)}` : "";
      const line = `[${time}] [${level}] ${message}${metaStr}`;

      // Console output (Render captures this)
      switch (level) {
        case "ERROR":
        case "FATAL":
          console.error(line);
          break;
        case "WARN":
          console.warn(line);
          break;
        default:
          console.log(line);
      }

      // File output (local runtime log file)
      if (this.stream) {
        this.stream.write(line + "\n");
      } else {
        fs.appendFile(
          path.join(LOG_DIR, `${this.currentDate || new Date().toISOString().slice(0, 10)}.log`),
          line + "\n",
          () => { }
        );
      }

      // Database persistence (mitfloww.worker_logs)
      if (dbPool) {
        const jobId = meta?.jobId || meta?.id || null;
        const fileId = meta?.fileId || null;
        const stage = meta?.stage || null;
        const queueName = meta?.queueName || null;
        const durationMs = typeof meta?.durationMs === 'number' ? meta.durationMs : null;
        const errorMessage = meta?.error instanceof Error ? meta.error.message : (meta?.errorMessage || null);
        const stackTrace = meta?.error instanceof Error ? meta.error.stack : null;

        dbPool.query(
          `INSERT INTO mitfloww.worker_logs (
            timestamp, level, event, message, job_id, file_id, stage, queue_name, duration_ms, error_message, stack_trace, metadata
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);`,
          [
            time,
            level.toLowerCase(),
            stage || 'worker_event',
            message,
            jobId,
            fileId,
            stage,
            queueName,
            durationMs,
            errorMessage,
            stackTrace,
            meta ? safeSerialize(meta) : null,
          ]
        ).catch(() => {
          // Failure isolation: never crash worker if log persistence fails
        });
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
