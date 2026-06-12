import net from "node:net";
import { HOST } from "./config.js";

// Start the listener on the given port; the first incoming socket is the
// predecessor in the ring, then the server stops accepting new connections
// (each node has exactly one predecessor).
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

// Client connection to the successor: retries until the peer is listening,
// since the four processes may be started in any order and there is no
// guarantee the successor has already called listen.
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
