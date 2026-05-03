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

// Id del successore secondo la topologia ad anello (ATM4 -> ATM1).
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

  // Log di startup: il nodo è up e dichiara su quale porta è in ascolto
  // e a quale successore si collegherà. Utile in avvio manuale, dove i
  // terminali di ATM2/3/4 resterebbero altrimenti muti finché ATM1 non
  // inietta il primo token.
  printToConsole(
    `starting on ${HOST}:${port} (successor: ATM${succId} on ${HOST}:${succ})`,
  );

  const incomingP = acceptFirstConnection(port);
  const outgoingP = connectWithRetry(succ);
  const [fromPred, toSucc] = await Promise.all([incomingP, outgoingP]);

  // Ring chiuso: i due lati della connessione (predecessore e successore)
  // sono pronti. ATM1 da qui inietta il token, gli altri restano in attesa.
  if (atmId === 1) {
    printToConsole(`connected to ring, injecting initial token`);
  } else {
    printToConsole(`connected to ring, waiting for token`);
  }

  const decoder = new RingMessageDecoder();
  const queue = [...pending];

  // Memoria privata del nodo: ogni ATM ha la sua copia del saldo.
  // Viene riallineata al valore portato dal token a ogni giro (vedi onMessage).
  let balance = INITIAL_BALANCE;

  // Chiusura ordinata dei socket. Non chiamiamo process.exit: lasciamo
  // che l'event loop si svuoti naturalmente quando i socket sono chiusi,
  // così eventuali write pendenti hanno il tempo di flushare.
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
    // Sezione critica del Token Ring: solo chi possiede il token può
    // leggere/scrivere il saldo. Riallineo la copia locale al valore
    // portato dal token (gli altri nodi potrebbero averlo modificato
    // mentre il token non era qui), eseguo eventualmente una transazione,
    // poi rilascio passando il token al successore.
    balance = msg.balance;
    const op = queue.shift();
    let idleHops: number;

    if (op !== undefined) {
      // Log didascalici allineati ai cinque step della specifica §5.2:
      // 1. lettura saldo, 2. validazione, 3-4. aggiornamento e scrittura,
      // 5. registrazione (log) dell'operazione. La sezione critica è
      // segnata esplicitamente in apertura e chiusura per rendere
      // visibile nei log l'invariante "una sola sezione critica attiva
      // alla volta" (specifica §9).
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
      // Una transazione è avvenuta: il ring NON è idle. Resetto il
      // contatore prima di rilanciare il token.
      idleHops = 0;
    } else {
      // Nessuna transazione in coda: incremento idleHops sul token (un hop
      // idle in più). Quando si raggiungono ATM_COUNT hop idle consecutivi
      // senza alcuna transazione, un nodo immette DONE (vedi sotto).
      idleHops = msg.idleHops + 1;
      printToConsole(
        `token received, current balance ${balance} (no pending transactions, idle ${idleHops}/${ATM_COUNT})`,
      );
    }

    // Pausa prima dell'inoltro (config: FORWARD_DELAY_MS) per rendere
    // leggibile la circolazione del token nei log e nel video.
    await sleep(FORWARD_DELAY_MS);

    // Rilevazione di terminazione distribuita: idleHops >= ATM_COUNT immette
    // un DONE col mio id come origine. Non esco subito: aspetto che il DONE
    // torni a me dopo aver fatto il giro dell'anello, così so che tutti gli
    // altri lo hanno visto e propagato.
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

  // Mutex implicito sui messaggi in arrivo: incateno ogni nuovo messaggio
  // su una Promise sequenziale, così due chiamate a onMessage non possono
  // mai sovrapporsi anche se TCP dovesse consegnare più messaggi prima
  // che il primo sia stato processato.
  // Nel Token Ring nominale (un solo testimone in circolazione) la
  // sovrapposizione non si presenta, ma serializzare qui rende la logica
  // robusta a evoluzioni future del protocollo (es. burst DONE+TOKEN) e
  // costa essenzialmente zero.
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

  // ATM1 è il seme del ring: inietta il primo TOKEN. Lo facciamo passare
  // per la stessa coda usata dai messaggi in ingresso, così anche al
  // primo giro vengono prodotti gli stessi log degli altri nodi e la
  // logica di terminazione vale anche per il nodo seme.
  if (atmId === 1) {
    const seed: TokenMessage = {
      type: "TOKEN",
      balance: INITIAL_BALANCE,
      idleHops: 0,
    };
    enqueue(seed);
  }
};
