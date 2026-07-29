import { connection } from "../queue/connection";
import { REDIS_KEYS } from "../constants";
import { logger } from "../utils/logger";

const CALLBACK_RETRY_INTERVAL_MS = 60_000;

let timer: NodeJS.Timeout | null = null;
let running = false;

async function retryPendingCallbacks() {
  if (running) {
    return;
  }

  running = true;

  try {
    const keys = await connection.keys("pending_callback:*");

    for (const key of keys) {
      try {
        const raw = await connection.get(key);

        if (!raw) {
          continue;
        }

        const payload = JSON.parse(raw);
        const jobId = key.replace("pending_callback:", "");

        const meta = await connection.hgetall(REDIS_KEYS.JOB(jobId));

        if (!meta.callbackUrl) {
          continue;
        }

        const response = await fetch(meta.callbackUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${
              process.env.PROCESSING_CALLBACK_TOKEN ||
              meta.callbackToken ||
              ""
            }`,
          },
          body: JSON.stringify({
            jobId,
            fileId: payload.fileId ?? jobId,
            fileVersionId: meta.fileVersionId,
            ...payload,
          }),
        });

        if (response.ok) {
          await connection.del(key);

          logger.info("Recovered processing callback", {
            jobId,
          });
        } else {
          logger.warn("Callback retry failed", {
            jobId,
            status: response.status,
          });
        }
      } catch (error) {
        logger.error("Callback retry error", {
          key,
          error,
        });
      }
    }
  } finally {
    running = false;
  }
}

export function startCallbackRetryWorker() {
  if (timer) {
    return;
  }

  logger.info("Starting callback retry worker");

  timer = setInterval(() => {
    void retryPendingCallbacks();
  }, CALLBACK_RETRY_INTERVAL_MS);

  // Run immediately on startup instead of waiting one minute.
  void retryPendingCallbacks();
}

export function stopCallbackRetryWorker() {
  if (!timer) {
    return;
  }

  clearInterval(timer);
  timer = null;

  logger.info("Stopped callback retry worker");
}