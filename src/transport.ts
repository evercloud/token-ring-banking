import net from "node:net";
import { HOST } from "./config.js";

// Listen on port; resolve with the first inbound socket (predecessor in the ring), then stop accepting.
export const acceptFirstConnection = (port: number): Promise<net.Socket> => {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(port, HOST, () => {
      server.once("connection", (socket: net.Socket) => {
        server.close();
        resolve(socket);
      });
    });
  });
};

type RetryOpts = {
  maxAttempts: number;
  delayMs: number;
};

const defaultRetry: RetryOpts = { maxAttempts: 150, delayMs: 100 };

// Client to successor: retry until the peer is listening (four terminals start in any order).
export const connectWithRetry = async (
  port: number,
  opts: RetryOpts = defaultRetry,
): Promise<net.Socket> => {
  for (let attempt = 0; attempt < opts.maxAttempts; attempt++) {
    try {
      const socket = await new Promise<net.Socket>((resolve, reject) => {
        const s = net.connect({ port, host: HOST }, () => {
          s.removeAllListeners("error");
          resolve(s);
        });
        s.once("error", reject);
      });
      return socket;
    } catch {
      await new Promise((r) => setTimeout(r, opts.delayMs));
    }
  }
  throw new Error(`could not connect to ${HOST}:${port}`);
};
