import {
  type RingMessage,
  type TokenMessage,
  RingMessageDecoder,
  serializeTokenMessage,
  serializeDoneMessage,
} from "./protocol.js";
import {
  ATM_COUNT,
  FORWARD_DELAY_MS,
  HOST,
  INITIAL_BALANCE,
  listenPort,
  successorPort,
} from "./config.js";
import { acceptFirstConnection, connectWithRetry } from "./transport.js";

export type TxOp =
  | { kind: "withdraw"; amount: number }
  | { kind: "deposit"; amount: number };

const applyOp = (balance: number, op: TxOp): number => {
  if (op.kind === "deposit") {
    return balance + op.amount;
  }
  if (op.amount > balance) {
    return balance;
  }
  return balance - op.amount;
};

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

// Successor id according to ring topology (ATM4 -> ATM1).
const successorId = (atmId: number): number =>
  atmId >= ATM_COUNT ? 1 : atmId + 1;

export const startAtm = async (
  atmId: number,
  pending: TxOp[],
): Promise<void> => {
  const printToConsole = (line: string): void => {
    console.log(`[ATM${atmId}] ${line}`);
  };

  const port = listenPort(atmId);
  const succ = successorPort(atmId);
  const succId = successorId(atmId);

  // Startup log: the node is up and declares which port it listens on
  // and which successor it will connect to. Useful in manual startup, where
  // ATM2/3/4 terminals would otherwise stay silent until ATM1 injects
  // the first token.
  printToConsole(
    `starting on ${HOST}:${port} (successor: ATM${succId} on ${HOST}:${succ})`,
  );

  const incomingP = acceptFirstConnection(port);
  const outgoingP = connectWithRetry(succ);
  const [fromPred, toSucc] = await Promise.all([incomingP, outgoingP]);

  // Ring closed: both sides of the connection (predecessor and successor)
  // are ready. ATM1 injects the token from here; the others wait.
  if (atmId === 1) {
    printToConsole(`connected to ring, injecting initial token`);
  } else {
    printToConsole(`connected to ring, waiting for token`);
  }

  const decoder = new RingMessageDecoder();
  const queue = [...pending];

  // Private node memory: each ATM has its own copy of the balance.
  // Realigned to the value carried by the token on each lap (see onMessage).
  let balance = INITIAL_BALANCE;

  // Orderly socket shutdown. We do not call process.exit: let the event
  // loop drain naturally when sockets are closed, so pending writes have
  // time to flush.
  const shutdown = (): void => {
    if (!toSucc.writableEnded) {
      toSucc.end();
    }
    fromPred.destroy();
  };

  const onMessage = async (msg: RingMessage): Promise<void> => {
    // ---- DONE: coordinated ring shutdown -----------------------------
    // A DONE in circulation means "the ring has detected termination".
    // Each node forwards it to its successor and exits; the origin
    // recognizes it when it comes back (origin == atmId) and closes
    // without forwarding.
    if (msg.type === "DONE") {
      balance = msg.balance;
      if (msg.origin === atmId) {
        printToConsole(`shutdown completed, final balance ${balance}`);
        shutdown();
        return;
      }
      printToConsole(
        `shutdown signal from ATM${msg.origin}, propagating (balance ${balance})`,
      );
      toSucc.end(serializeDoneMessage(balance, msg.origin));
      fromPred.destroy();
      return;
    }

    // ---- Normal TOKEN ---------------------------------------------------
    // Token Ring critical section: only the token holder can read/write
    // the balance. Realign the local copy to the value carried by the token
    // (other nodes may have modified it while the token was elsewhere),
    // optionally execute a transaction, then release by passing the token
    // to the successor.
    balance = msg.balance;
    const op = queue.shift();
    let idleHops: number;

    if (op !== undefined) {
      // Explanatory logs aligned with the five steps of spec §5.2:
      // 1. read balance, 2. validation, 3-4. update and write,
      // 5. log the operation. The critical section is explicitly marked
      // at entry and exit to make the "only one active critical section
      // at a time" invariant visible in the logs (spec §9).
      printToConsole(`token received, current balance ${balance}`);
      printToConsole(`entering critical section`);
      printToConsole(`start transaction: ${op.kind} ${op.amount}`);
      const before = balance;
      balance = applyOp(balance, op);
      if (op.kind === "withdraw" && balance === before) {
        printToConsole(
          `validation FAILED (insufficient funds, balance ${balance})`,
        );
        printToConsole(`transaction aborted, balance unchanged`);
      } else {
        printToConsole(`validation OK`);
        printToConsole(`balance updated: ${before} -> ${balance}`);
        printToConsole(`transaction logged`);
      }
      printToConsole(`leaving critical section`);
      // A transaction occurred: the ring is NOT idle. Reset the counter
      // before forwarding the token.
      idleHops = 0;
    } else {
      // No queued transaction: increment idleHops on the token (one more
      // idle hop). When ATM_COUNT consecutive idle hops are reached without
      // any transaction, a node injects DONE (see below).
      idleHops = msg.idleHops + 1;
      printToConsole(
        `token received, current balance ${balance} (no pending transactions, idle ${idleHops}/${ATM_COUNT})`,
      );
    }

    // Pause before forwarding (config: FORWARD_DELAY_MS) to make token
    // circulation easier to follow in logs and videos.
    await sleep(FORWARD_DELAY_MS);

    // Distributed termination detection: idleHops >= ATM_COUNT injects a
    // DONE with my id as origin. Do not exit immediately: wait for DONE to
    // come back after a full ring lap, so I know everyone else has seen
    // and propagated it.
    if (idleHops >= ATM_COUNT) {
      printToConsole(
        `ring idle for ${idleHops} hops, initiating coordinated shutdown (origin ATM${atmId})`,
      );
      toSucc.write(serializeDoneMessage(balance, atmId));
      return;
    }

    printToConsole(`forward token, balance ${balance}`);
    toSucc.write(serializeTokenMessage(balance, idleHops));
  };

  // Implicit mutex on incoming messages: chain each new message onto a
  // sequential Promise, so two onMessage calls can never overlap even if
  // TCP delivers multiple messages before the first one is processed.
  // In the nominal Token Ring (a single token in circulation) overlap
  // does not occur, but serializing here keeps the logic robust to future
  // protocol changes (e.g. DONE+TOKEN bursts) at essentially zero cost.
  let processing: Promise<void> = Promise.resolve();
  const enqueue = (msg: RingMessage): void => {
    processing = processing
      .then(() => onMessage(msg))
      .catch((err: unknown) => {
        console.error(err);
      });
  };

  fromPred.on("data", (buf: Buffer) => {
    for (const msg of decoder.append(buf.toString("utf8"))) {
      enqueue(msg);
    }
  });

  fromPred.on("end", () => {
    for (const msg of decoder.flush()) {
      enqueue(msg);
    }
  });

  // ATM1 is the ring seed: injects the first TOKEN. Route it through the
  // same queue used for incoming messages, so even on the first lap the
  // same logs are produced as on other nodes and termination logic applies
  // to the seed node too.
  if (atmId === 1) {
    const seed: TokenMessage = {
      type: "TOKEN",
      balance: INITIAL_BALANCE,
      idleHops: 0,
    };
    enqueue(seed);
  }
};
