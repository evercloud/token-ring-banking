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

  const decoder = new RingMessageDecoder();
  const queue = [...pending];

  // Memoria privata del nodo: ogni ATM ha la sua copia del saldo.
  // Viene riallineata al valore portato dal token a ogni giro (vedi onMessage).
  let balance = INITIAL_BALANCE;

  // Chiusura ordinata dei socket. Non chiamiamo process.exit: lasciamo
  // che l'event loop si svuoti naturalmente quando i socket sono chiusi,
  // cosi' eventuali write pendenti hanno il tempo di flushare.
  const shutdown = (): void => {
    if (!toSucc.writableEnded) {
      toSucc.end();
    }
    fromPred.destroy();
  };

  const onMessage = async (msg: RingMessage): Promise<void> => {
    // ---- DONE: shutdown coordinato del ring -----------------------------
    // Un DONE in circolazione significa "il ring ha rilevato terminazione".
    // Ogni nodo lo inoltra al successore ed esce; l'origine lo riconosce
    // quando torna a lei (origin == atmId) e chiude senza inoltrare.
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

    // ---- TOKEN normale ---------------------------------------------------
    // Sezione critica del Token Ring: solo chi possiede il token puo'
    // leggere/scrivere il saldo. Riallineo la copia locale al valore
    // portato dal token (gli altri nodi potrebbero averlo modificato
    // mentre il token non era qui), eseguo eventualmente una transazione,
    // poi rilascio passando il token al successore.
    balance = msg.balance;
    const op = queue.shift();
    let quietRounds: number;

    if (op !== undefined) {
      printToConsole(`token arrived, balance ${balance}`);
      printToConsole(`start ${op.kind} ${op.amount}`);
      const before = balance;
      balance = applyOp(balance, op);
      if (op.kind === "withdraw" && balance === before) {
        printToConsole(`withdraw skipped (not enough money), balance ${balance}`);
      } else {
        printToConsole(`after tx, balance ${balance}`);
      }
      printToConsole(`transaction completed`);
      // Una transazione e' avvenuta: il ring NON e' idle. Resetto il
      // contatore prima di rilanciare il token.
      quietRounds = 0;
    } else {
      // Nessuna transazione in coda: incremento il contatore di giri
      // idle portato dal token. Quando ATM_COUNT nodi consecutivi non
      // fanno modifiche, il giro e' completo senza attivita' -> termino.
      quietRounds = msg.quietRounds + 1;
      printToConsole(
        `token arrived (idle ${quietRounds}/${ATM_COUNT}), balance ${balance}`,
      );
    }

    // Pausa prima dell'inoltro (config: FORWARD_DELAY_MS) per rendere
    // leggibile la circolazione del token nei log e nel video.
    await sleep(FORWARD_DELAY_MS);

    // Rilevazione di terminazione distribuita: se ho appena chiuso un
    // intero giro idle, immetto un DONE col mio id come origine. Non
    // esco subito: aspetto che il DONE torni a me dopo aver fatto il
    // giro, cosi' so che tutti gli altri lo hanno visto e propagato.
    if (quietRounds >= ATM_COUNT) {
      printToConsole(
        `ring idle for ${quietRounds} hops, initiating shutdown (origin ATM${atmId})`,
      );
      toSucc.write(serializeDoneMessage(balance, atmId));
      return;
    }

    printToConsole(`forward token, balance ${balance}`);
    toSucc.write(serializeTokenMessage(balance, quietRounds));
  };

  // Le callback di "data"/"end" sono async: in un Token Ring c'e' un
  // solo testimone in circolazione, quindi non possono arrivare due
  // messaggi mentre ne sto elaborando uno (il predecessore non
  // rispedira' finche' il token non gli torna), e non c'e' rischio
  // di chiamate sovrapposte di onMessage.
  fromPred.on("data", async (buf: Buffer) => {
    for (const msg of decoder.append(buf.toString("utf8"))) {
      await onMessage(msg);
    }
  });

  fromPred.on("end", async () => {
    for (const msg of decoder.flush()) {
      await onMessage(msg);
    }
  });

  // ATM1 e' il seme del ring: inietta il primo TOKEN. Lo facciamo passare
  // per onMessage (invece di scrivere il TOKEN direttamente) cosi' anche
  // al primo giro vengono prodotti gli stessi log degli altri nodi e la
  // logica di terminazione vale anche per il nodo seme.
  if (atmId === 1) {
    const seed: TokenMessage = {
      type: "TOKEN",
      balance: INITIAL_BALANCE,
      quietRounds: 0,
    };
    void onMessage(seed);
  }
};
