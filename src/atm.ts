import {
  type TokenMessage,
  serializeTokenMessage,
  TokenLineDecoder,
} from "./protocol.js";
import {
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

export const startAtm = async (
  atmId: number,
  pending: TxOp[],
): Promise<void> => {
  const printToConsole = (line: string): void => {
    console.log(`[ATM${atmId}] ${line}`);
  };

  const port = listenPort(atmId);
  const succ = successorPort(atmId);

  const incomingP = acceptFirstConnection(port);
  const outgoingP = connectWithRetry(succ);
  const [fromPred, toSucc] = await Promise.all([incomingP, outgoingP]);

  const decoder = new TokenLineDecoder();
  const queue = [...pending];

  const forward = (balance: number): void => {
    toSucc.write(serializeTokenMessage(balance));
  };

  const onToken = (msg: TokenMessage): void => {
    let balance = msg.balance;
    printToConsole(`token arrived, balance ${balance}`);

    if (queue.length > 0) {
      const op = queue.shift()!;
      printToConsole(`start ${op.kind} ${op.amount}`);
      const before = balance;
      balance = applyOp(balance, op);
      if (op.kind === "withdraw" && balance === before) {
        printToConsole(`withdraw skipped (not enough money), balance ${balance}`);
      } else {
        printToConsole(`after tx, balance ${balance}`);
      }
    }

    printToConsole(`forward token, balance ${balance}`);
    forward(balance);
  };

  fromPred.on("data", (buf: Buffer) => {
    for (const msg of decoder.append(buf.toString("utf8"))) {
      onToken(msg);
    }
  });

  fromPred.on("end", () => {
    for (const msg of decoder.flush()) {
      onToken(msg);
    }
  });

  if (atmId === 1) {
    printToConsole(`forward token, balance ${INITIAL_BALANCE}`);
    forward(INITIAL_BALANCE);
  }
};
