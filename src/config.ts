export const ATM_COUNT = 4;
export const INITIAL_BALANCE = 1000;

// Stessa stringa host per listen e connect: evita disallineamenti
// fra la risoluzione IPv4 e IPv6 di "localhost" su alcuni sistemi.
export const HOST = "127.0.0.1";

export const BASE_PORT = 4000;

// Pausa tra la ricezione e l'inoltro del token. Senza questo ritardo
// il token gira a velocità CPU e i log diventano illeggibili.
export const FORWARD_DELAY_MS = 500;

// ATM k ascolta sulla porta BASE_PORT + k (es. ATM1 -> 4001).
export const listenPort = (atmId: number): number => BASE_PORT + atmId;

// Il successore di ATM k è ATM k+1, con ATM4 che chiude il ring
// tornando a ATM1.
export const successorPort = (atmId: number): number => {
  const next = atmId >= ATM_COUNT ? 1 : atmId + 1;
  return BASE_PORT + next;
};

export const assertAtmId = (n: number): void => {
  if (!Number.isInteger(n) || n < 1 || n > ATM_COUNT) {
    throw new Error(`atm id must be an integer in 1..${ATM_COUNT}`);
  }
};
