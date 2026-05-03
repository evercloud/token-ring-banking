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

  // Memoria privata del nodo: ogni ATM ha la sua copia del saldo.
  // Viene riallineata al valore portato dal token a ogni giro (vedi onToken).
  let balance = INITIAL_BALANCE;

  const forward = (): void => {
    toSucc.write(serializeTokenMessage(balance));
  };

  const onToken = (msg: TokenMessage): void => {
    // Sezione critica del Token Ring: solo chi possiede il token può
    // leggere/scrivere il saldo. Prima cosa, allineo la copia locale
    // al valore portato dal token (gli altri nodi potrebbero averlo
    // modificato mentre il token non era qui), poi eventualmente eseguo
    // una transazione e infine rilascio passando il token al successore.
    balance = msg.balance;
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
      printToConsole(`transaction completed`);
    }

    printToConsole(`forward token, balance ${balance}`);
    forward();
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

  // ATM1 è il seme del ring: inietta il primo token. Lo facciamo passare
  // per onToken (invece di chiamare forward direttamente) così anche al
  // primo giro vengono prodotti gli stessi log degli altri nodi
  // ("token ricevuto / inoltro token"), come da esempio nella specifica.
  if (atmId === 1) {
    onToken({ type: "TOKEN", balance: INITIAL_BALANCE });
  }
};
