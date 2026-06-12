export const ATM_COUNT = 4;
export const INITIAL_BALANCE = 1000;

// Same host string for listen and connect: avoids mismatches between
// IPv4 and IPv6 resolution of "localhost" on some systems.
export const HOST = "127.0.0.1";

export const BASE_PORT = 4000;

// Pause between receiving and forwarding the token. Without this delay
// the token spins at CPU speed and logs become unreadable.
export const FORWARD_DELAY_MS = 500;

// ATM k listens on port BASE_PORT + k (e.g. ATM1 -> 4001).
export const listenPort = (atmId: number): number => BASE_PORT + atmId;

// The successor of ATM k is ATM k+1, with ATM4 closing the ring
// by returning to ATM1.
export const successorPort = (atmId: number): number => {
  const next = atmId >= ATM_COUNT ? 1 : atmId + 1;
  return BASE_PORT + next;
};

export const assertAtmId = (n: number): void => {
  if (!Number.isInteger(n) || n < 1 || n > ATM_COUNT) {
    throw new Error(`atm id must be an integer in 1..${ATM_COUNT}`);
  }
};
