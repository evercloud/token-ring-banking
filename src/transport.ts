import net from "node:net";
import { HOST } from "./config.js";

// Avvia il listener sulla porta data; il primo socket entrante è il
// predecessore nel ring, poi il server smette di accettare
// nuove connessioni (un nodo ha esattamente un predecessore).
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

// Connessione client al successore: ritenta finché il peer non è in
// ascolto, dato che i quattro processi possono essere avviati in qualunque
// ordine e non c'è garanzia che il successore abbia già fatto il listen.
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
