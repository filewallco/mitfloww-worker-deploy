import { WebSocketServer } from 'ws';
import { getSystemSnapshot } from './admin';
import { isWebSocketRequestAuthorized } from '../security/auth';
import { config } from '../config';

let wssInstance: WebSocketServer | null = null;
let wsInterval: NodeJS.Timeout | null = null;

/**
 * WebSocket server for real-time updates
 */
export function startWS() {
  if (wssInstance) return wssInstance;

  const wsPort = process.env.WS_PORT ? Number(process.env.WS_PORT) : 4002;
  const wss = new WebSocketServer({
    port: wsPort,
    verifyClient: ({ req }, done) => {
      done(isWebSocketRequestAuthorized(req), 401, 'Unauthorized');
    },
  });

  wsInterval = setInterval(async () => {
    try {
      const data = JSON.stringify(await getSystemSnapshot());

      wss.clients.forEach((client: any) => {
        if (client.readyState === 1) {
          client.send(data);
        }
      });
    } catch {}
  }, config.wsSnapshotIntervalMs);

  console.log(`WebSocket running on ws://0.0.0.0:${wsPort}`);
  wssInstance = wss;
  return wss;
}

export function stopWS(): Promise<void> {
  return new Promise((resolve) => {
    if (wsInterval) {
      clearInterval(wsInterval);
      wsInterval = null;
    }

    if (!wssInstance) {
      resolve();
      return;
    }

    wssInstance.close(() => {
      wssInstance = null;
      resolve();
    });
  });
}
