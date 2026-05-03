export const ATM_COUNT = 4;
export const INITIAL_BALANCE = 1000;

// Same host string for listen and connect (avoids localhost IPv4/IPv6 mismatch).
export const HOST = "127.0.0.1";

export const BASE_PORT = 4000;

// ATM k listens on BASE_PORT + k (e.g. ATM1 -> 4001).
export const listenPort = (atmId: number): number => BASE_PORT + atmId;

// Successor of ATM k is k+1, except ATM4 -> ATM1.
export const successorPort = (atmId: number): number => {
  const next = atmId >= ATM_COUNT ? 1 : atmId + 1;
  return BASE_PORT + next;
};

export const assertAtmId = (n: number): void => {
  if (!Number.isInteger(n) || n < 1 || n > ATM_COUNT) {
    throw new Error(`atm id must be an integer in 1..${ATM_COUNT}`);
  }
};
