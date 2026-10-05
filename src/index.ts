import 'dotenv/config';
import { config } from './config';
import { recoverStuckJobs } from './server/admin';
import { startAdminServer, stopAdminServer } from './server/http';
import { startWS, stopWS } from './server/ws';
import { getFreeDiskSpace } from './utils/disk';
import { cleanupTempDir, runPeriodicMaintenance } from './utils/cleanup';
import { logger } from './utils/logger';
import { connection } from './queue/connection';

// Import workers to initialize queues
import { fastWorker } from './worker/fastWorker';
import { standardWorker } from './worker/standardWorker';
import { heavyWorker } from './worker/heavyWorker';
import { imageWorker } from './worker/imageWorker';
import { startCallbackRetryWorker, stopCallbackRetryWorker } from './worker/callbackRetry';
import { startPriorityScheduler, stopPriorityScheduler } from './scheduler/priorityScheduler';
import { reconcileResourceHolders } from './worker/resourceManager';

process.env.SESSION_ID = Date.now().toString();

logger.info("Worker starting", {
  mode: config.mode,
  node: process.version,
  sessionId: process.env.SESSION_ID,
  redisUrl: !!process.env.REDIS_URL,
});

/**
 * Entry point for MitFloww.
 *
 * Server mode:
 * - Starts workers
 * - Starts HTTP API
 * - Waits for Next.js to enqueue jobs through POST /jobs
 *
 * Local test mode:
 * - Only runs localTest when explicitly enabled
 */
if (config.mode !== "local" && config.mode !== "server") {
  throw new Error(`Invalid MODE: ${config.mode}`);
}

if (config.mode === "local" && process.env.ENABLE_LOCAL_TEST === "true") {
  require("./localTest");
}

logger.info(`Running in ${config.mode} mode`, { mode: config.mode });

try {
  logger.info("Starting admin server");
  startAdminServer();

  logger.info("Starting WebSocket server");
  startWS();

  logger.info("Starting priority scheduler");
  startPriorityScheduler();

  startCallbackRetryWorker();

  logger.info("Worker startup complete");

  reconcileResourceHolders().catch((err) =>
    logger.error('reconcileResourceHolders on startup failed', { error: err }),
  );
  recoverStuckJobs().catch((err) =>
    logger.error('recoverStuckJobs on startup failed', { error: err }),
  );
} catch (err) {
  logger.fatal("Startup failed", { error: err });
  process.exit(1);
}

const reconcileInterval = setInterval(() => {
  reconcileResourceHolders().catch((err) =>
    logger.error('reconcileResourceHolders failed', { error: err }),
  );
  recoverStuckJobs().catch((err) => logger.error('recoverStuckJobs failed', { error: err }));
}, 60000); // every 1 min

/**
 * Background disk manager
 */
const diskCleanupInterval = setInterval(async () => {
  try {
    const free = getFreeDiskSpace();

    if (free < config.disk.targetFreeBytes) {
      logger.info('Running disk cleanup, free space below target', { freeBytes: free, targetBytes: config.disk.targetFreeBytes });
      await cleanupTempDir();
    }
  } catch (err) {
    logger.error('Cleanup error', { error: err });
  }
}, 60_000); // every 1 min

/**
 * Periodic maintenance: runs every 12 hours (purging 72h / 3-day old orphaned data)
 */
const maintenanceInterval = setInterval(async () => {
  try {
    await runPeriodicMaintenance();
  } catch (err) {
    logger.error('Periodic maintenance scheduled run failed', { error: err });
  }
}, 12 * 60 * 60 * 1000);

const maintenanceTimeout = setTimeout(() => {
  runPeriodicMaintenance().catch((err) =>
    logger.error('Startup periodic maintenance error', { error: err }),
  );
}, 60_000);

let isShuttingDown = false;

async function handleShutdown(signal: string) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info(`Received ${signal}, initiating graceful shutdown...`, { signal });

  // Force exit fallback timer in case active jobs don't complete within 60s
  const forceExitTimer = setTimeout(() => {
    logger.error('Graceful shutdown timed out after 60 seconds, forcing exit');
    process.exit(1);
  }, 60_000);
  forceExitTimer.unref();

  try {
    // 1. Clear recurring timers
    clearInterval(reconcileInterval);
    clearInterval(diskCleanupInterval);
    clearInterval(maintenanceInterval);
    clearTimeout(maintenanceTimeout);
    stopPriorityScheduler();
    stopCallbackRetryWorker();

    // 2. Stop accepting new jobs and wait for in-flight jobs in BullMQ workers
    logger.info('Closing BullMQ workers (waiting for active jobs)...');
    await Promise.allSettled([
      fastWorker.close(),
      standardWorker.close(),
      heavyWorker.close(),
      imageWorker.close(),
    ]);
    logger.info('BullMQ workers closed cleanly');

    // 3. Stop HTTP and WebSocket servers
    logger.info('Stopping HTTP admin server and WebSocket server...');
    await Promise.allSettled([
      stopAdminServer(),
      stopWS(),
    ]);

    // 4. Run cleanup on any remaining temp files
    try {
      await cleanupTempDir();
    } catch (cleanupErr) {
      logger.warn('Cleanup during shutdown encountered error', { error: cleanupErr });
    }

    // 5. Close Redis connection
    logger.info('Closing Redis connection...');
    try {
      await connection.quit();
      logger.info('Redis connection closed cleanly');
    } catch (redisErr) {
      logger.warn('Error closing Redis connection', { error: redisErr });
    }

    logger.info('Graceful shutdown completed successfully. Exiting.');
    process.exit(0);
  } catch (err) {
    logger.error('Error during graceful shutdown', { error: err });
    process.exit(1);
  }
}

process.on('SIGTERM', () => void handleShutdown('SIGTERM'));
process.on('SIGINT', () => void handleShutdown('SIGINT'));

process.on('uncaughtException', (err) => {
  try {
    logger.fatal('uncaughtException', { error: err, sessionId: process.env.SESSION_ID });
  } catch {}
});

process.on('unhandledRejection', (reason) => {
  try {
    logger.fatal('unhandledRejection', { reason, sessionId: process.env.SESSION_ID });
  } catch {}
});
