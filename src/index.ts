import 'dotenv/config';
import { config } from './config';
import { recoverStuckJobs } from './server/admin';
import { startAdminServer } from './server/http';
import { startWS } from './server/ws';
import { getFreeDiskSpace } from './utils/disk';
import { cleanupTempDir } from './utils/cleanup';
import { logger } from './utils/logger';

// Import workers to initialize queues
import './worker/fastWorker';
import './worker/standardWorker';
import './worker/heavyWorker';
import './worker/imageWorker';
import { startPriorityScheduler } from './scheduler/priorityScheduler';

process.env.SESSION_ID = Date.now().toString();

/**
 * Entry point for MitFloww.
 * - Server mode: starts HTTP server.
 * - Local mode: runs test runner for enqueuing jobs.
 */
if (config.mode === 'local') {
  require('./localTest');
} else if (config.mode === 'server') {
  const { startServer } = require('./server');
  startServer();
} else {
  throw new Error(`Invalid MODE: ${config.mode}`);
}

logger.info(`Running in ${config.mode} mode`, { mode: config.mode });

startAdminServer();
startWS();
startPriorityScheduler();

setInterval(() => {
  recoverStuckJobs().catch((err) => logger.error('recoverStuckJobs failed', { error: err }));
}, 60000); // every 1 min

/**
 * Background disk manager
 */
setInterval(async () => {
  try {
    const free = getFreeDiskSpace();

    if (free < config.disk.targetFreeBytes) {
      console.log('Running disk cleanup...');
      await cleanupTempDir();
    }
    } catch (err) {
    logger.error('Cleanup error', { error: err });
  }
}, 60_000); // every 1 min

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