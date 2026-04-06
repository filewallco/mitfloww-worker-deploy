import dotenv from 'dotenv';
dotenv.config({ path: '.env.local' });

import { recoverStuckJobs } from './server/admin';
import { startAdminServer } from './server/http';
import { startWS } from './server/ws';

// Import workers to initialize queues
import './worker/fastWorker';
import './worker/standardWorker';
import './worker/heavyWorker';


process.env.SESSION_ID = Date.now().toString();
const MODE = (process.env.MODE as 'local' | 'server') || 'local';

/**
 * Entry point for FileWall.
 * - Server mode: starts HTTP server.
 * - Local mode: runs test runner for enqueuing jobs.
 */
if (MODE === 'server') {
  const { startServer } = require('./server');
  startServer();
} else {
  require('./localTest');
}

console.log(`Running in ${MODE} mode`);
startAdminServer();
startWS();

setInterval(() => {
  recoverStuckJobs().catch(console.error);
}, 60000); // every 1 min