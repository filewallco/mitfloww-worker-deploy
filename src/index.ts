import dotenv from 'dotenv';
dotenv.config({ path: '.env.local' });

import { recoverStuckJobs } from './server/admin';
import { startAdminServer } from './server/http';
import { startWS } from './server/ws';
import { getFreeDiskSpace } from './utils/disk';
import { config } from './config';
import { cleanupTempDir } from './utils/cleanup';

// Import workers to initialize queues
import './worker/fastWorker';
import './worker/standardWorker';
import './worker/heavyWorker';
import './worker/imageWorker';

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

console.log(`Running in ${config.mode} mode`);
if (process.env.WORKER_ONLY === 'true') {
  console.log('Starting WORKER only');
} else if (process.env.API_ONLY === 'true') {
  console.log('Starting API only');
  startAdminServer();
  startWS();
} else {
  console.log('Starting FULL app');
  startAdminServer();
  startWS();
}
startWS();

console.log("ACTUAL MODE:", process.env.MODE);

setInterval(() => {
  recoverStuckJobs().catch(console.error);
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
    console.error('Cleanup error:', err);
  }
}, 60_000); // every 1 min