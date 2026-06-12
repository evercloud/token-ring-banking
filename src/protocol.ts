// Wire format: one JSON object per TCP line (trailing newline).
// Two message types can travel on the ring:
//  - TOKEN: the normal token, carries the balance and the idleHops counter
//    (consecutive token hops without a balance transaction), used by distributed
//    termination detection.
//  - DONE: coordinated shutdown message. When a node detects that the ring has
//    been idle for ATM_COUNT consecutive hops, it injects a DONE marked with
//    its own id (origin). DONE makes a full lap: every node that receives it
//    forwards it and exits; the origin, when it receives its own DONE back,
//    knows everyone has propagated it and can exit as well.

export type TokenMessage = {
  type: "TOKEN";
  balance: number;
  idleHops: number;
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
  if (typeof o.idleHops !== "number" || !Number.isInteger(o.idleHops)) {
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
  idleHops: number,
): string => {
  const msg: TokenMessage = { type: "TOKEN", balance, idleHops };
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

// NDJSON decoder: turns TCP chunks into complete messages, line by line.
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
