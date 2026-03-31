import dotenv from 'dotenv';
dotenv.config({ path: '.env.local' });

import './worker/smallWorker';
import './worker/mediumWorker';
import './worker/largeWorker';

const MODE = (process.env.MODE as 'local' | 'server') || 'local';

if (MODE === 'server') {
  const { startServer } = require('./server');
  startServer();
} else {
  require('./localTest');
}

console.log(`Running in ${MODE} mode`);