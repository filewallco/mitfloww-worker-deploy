import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { config } from "../config";
import { logger } from "./logger";

export function getSourceCacheKey(input: {
  fileVersionId?: string;
  sourceKey?: string;
  fileId?: string;
}): string {
  if (input.fileVersionId && input.fileVersionId.trim()) {
    return input.fileVersionId.trim().replace(/[^a-zA-Z0-9_-]/g, "_");
  }
  if (input.sourceKey && input.sourceKey.trim()) {
    return crypto.createHash("sha256").update(input.sourceKey.trim()).digest("hex");
  }
  if (input.fileId && input.fileId.trim()) {
    return input.fileId.trim().replace(/[^a-zA-Z0-9_-]/g, "_");
  }
  return "unknown";
}

export function getSourceCachePath(cacheKey: string): string {
  return path.join(config.sourceCacheDir, `${cacheKey}.source`);
}

export async function deleteSourceCache(keyOrId: string): Promise<boolean> {
  if (!keyOrId || !keyOrId.trim()) return false;
  let deleted = false;
  const candidates = [
    getSourceCachePath(getSourceCacheKey({ fileVersionId: keyOrId })),
    getSourceCachePath(keyOrId.trim().replace(/[^a-zA-Z0-9_-]/g, "_")),
    getSourceCachePath(crypto.createHash("sha256").update(keyOrId.trim()).digest("hex")),
  ];

  for (const filePath of candidates) {
    try {
      if (fs.existsSync(filePath)) {
        await fs.promises.unlink(filePath);
        logger.info("Removed cached source file", { filePath });
        deleted = true;
      }
    } catch (err) {
      logger.error("Failed to delete cached source file", { filePath, err });
    }
  }

  return deleted;
}
