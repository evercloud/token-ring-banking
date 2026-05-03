// Wire format: un oggetto JSON per riga TCP (newline alla fine).
// Sul ring possono viaggiare due tipi di messaggio:
//  - TOKEN: il testimone normale, porta il saldo e il contatore di giri
//    consecutivi senza modifiche (quietRounds), usato dalla rilevazione
//    di terminazione distribuita.
//  - DONE: messaggio di shutdown coordinato. Quando un nodo rileva che
//    il ring e' rimasto idle per ATM_COUNT hop consecutivi, immette un
//    DONE marcato col proprio id (origin). Il DONE fa un giro completo:
//    ogni nodo che lo riceve lo inoltra ed esce; l'origine, quando
//    riceve di ritorno il proprio DONE, sa che tutti hanno propagato
//    e puo' uscire a sua volta.

export type TokenMessage = {
  type: "TOKEN";
  balance: number;
  quietRounds: number;
};

export type DoneMessage = {
  type: "DONE";
  balance: number;
  origin: number;
};

export type RingMessage = TokenMessage | DoneMessage;

const FRAME_SUFFIX = "\n";

export class ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

const isTokenMessage = (value: unknown): value is TokenMessage => {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const o = value as Record<string, unknown>;
  if (o.type !== "TOKEN") {
    return false;
  }
  if (typeof o.balance !== "number" || !Number.isFinite(o.balance)) {
    return false;
  }
  if (typeof o.quietRounds !== "number" || !Number.isInteger(o.quietRounds)) {
    return false;
  }
  return true;
};

const isDoneMessage = (value: unknown): value is DoneMessage => {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const o = value as Record<string, unknown>;
  if (o.type !== "DONE") {
    return false;
  }
  if (typeof o.balance !== "number" || !Number.isFinite(o.balance)) {
    return false;
  }
  if (typeof o.origin !== "number" || !Number.isInteger(o.origin)) {
    return false;
  }
  return true;
};

export const serializeTokenMessage = (
  balance: number,
  quietRounds: number,
): string => {
  const msg: TokenMessage = { type: "TOKEN", balance, quietRounds };
  return JSON.stringify(msg) + FRAME_SUFFIX;
};

export const serializeDoneMessage = (
  balance: number,
  origin: number,
): string => {
  const msg: DoneMessage = { type: "DONE", balance, origin };
  return JSON.stringify(msg) + FRAME_SUFFIX;
};

export const parseRingLine = (line: string): RingMessage => {
  const trimmed = line.trim();
  if (trimmed.length === 0) {
    throw new ProtocolError("empty line");
  }
  let raw: unknown;
  try {
    raw = JSON.parse(trimmed) as unknown;
  } catch {
    throw new ProtocolError("invalid JSON");
  }
  if (isTokenMessage(raw)) {
    return raw;
  }
  if (isDoneMessage(raw)) {
    return raw;
  }
  throw new ProtocolError("not a ring message");
};

// Decoder NDJSON: trasforma chunk TCP in messaggi completi, riga per riga.
export class RingMessageDecoder {
  private buffer = "";

  append(chunk: string): RingMessage[] {
    this.buffer += chunk;
    const parts = this.buffer.split("\n");
    this.buffer = parts.pop() ?? "";
    const out: RingMessage[] = [];
    for (const part of parts) {
      const t = part.trim();
      if (t.length === 0) {
        continue;
      }
      out.push(parseRingLine(t));
    }
    return out;
  }

  flush(): RingMessage[] {
    const tail = this.buffer.trim();
    this.buffer = "";
    if (tail.length === 0) {
      return [];
    }
    return [parseRingLine(tail)];
  }
}
