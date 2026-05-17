import { WebSocketServer } from 'ws';
import { getSystemSnapshot } from './admin';
import { isWebSocketRequestAuthorized } from '../security/auth';
import { config } from '../config';

/**
 * WebSocket server for real-time updates
 */
export function startWS() {
  const wss = new WebSocketServer({
    port: 4001,
    verifyClient: ({ req }, done) => {
      done(isWebSocketRequestAuthorized(req), 401, 'Unauthorized');
    },
  });

  setInterval(async () => {
    const data = JSON.stringify(await getSystemSnapshot());

    wss.clients.forEach((client: any) => {
      if (client.readyState === 1) {
        client.send(data);
      }
    });
  }, config.wsSnapshotIntervalMs);

  console.log('WebSocket running on ws://localhost:4001');
}
