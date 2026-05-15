import crypto from 'crypto';
import type { IncomingMessage } from 'http';
import { config } from '../config';

function safeCompare(actual: string | null, expected: string): boolean {
  if (!actual || !expected) return false;

  const actualBuffer = Buffer.from(actual);
  const expectedBuffer = Buffer.from(expected);

  return (
    actualBuffer.length === expectedBuffer.length &&
    crypto.timingSafeEqual(actualBuffer, expectedBuffer)
  );
}

function bearerToken(req: IncomingMessage): string | null {
  const header = req.headers.authorization;
  if (!header?.startsWith('Bearer ')) return null;
  return header.slice('Bearer '.length).trim();
}

export function isAdminRequestAuthorized(req: IncomingMessage): boolean {
  if (config.mode === 'local' && !config.adminToken) return true;
  return safeCompare(bearerToken(req), config.adminToken);
}

export function isWebSocketRequestAuthorized(req: IncomingMessage): boolean {
  const expectedToken = config.wsToken || config.adminToken;
  if (config.mode === 'local' && !expectedToken) return true;

  const requestUrl = new URL(req.url || '/', 'ws://localhost');
  return safeCompare(bearerToken(req) || requestUrl.searchParams.get('token'), expectedToken);
}
