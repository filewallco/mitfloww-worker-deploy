import { WebSocketServer } from 'ws';
import { getSystemSnapshot } from './admin';

/**
 * WebSocket server for real-time updates
 */
export function startWS() {
  const wss = new WebSocketServer({ port: 4001 });

  setInterval(async () => {
    const data = JSON.stringify(await getSystemSnapshot());

    wss.clients.forEach((client: any) => {
      if (client.readyState === 1) {
        client.send(data);
      }
    });
  }, 1000);

  console.log('WebSocket running on ws://localhost:4001');
}