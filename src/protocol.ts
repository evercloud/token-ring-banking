// One JSON object per TCP line (newline after each message).
// Current balance is inside the token message.

export type TokenMessage = {
  type: "TOKEN";
  balance: number;
};

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
  return true;
};

// Encode TOKEN as JSON plus newline.
export const serializeTokenMessage = (balance: number): string => {
  const msg: TokenMessage = { type: "TOKEN", balance };
  return JSON.stringify(msg) + FRAME_SUFFIX;
};

// Decode one line (JSON without the newline).
export const parseTokenLine = (line: string): TokenMessage => {
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
  if (!isTokenMessage(raw)) {
    throw new ProtocolError("not a TOKEN message");
  }
  return raw;
};

// Turn TCP chunks into full lines; skips empty lines.
export class TokenLineDecoder {
  private buffer = "";

  // Feed bytes from socket "data"; each complete line becomes one message.
  append(chunk: string): TokenMessage[] {
    this.buffer += chunk;
    const parts = this.buffer.split("\n");
    this.buffer = parts.pop() ?? "";
    const out: TokenMessage[] = [];
    for (const part of parts) {
      const t = part.trim();
      if (t.length === 0) {
        continue;
      }
      out.push(parseTokenLine(t));
    }
    return out;
  }

  // Last bytes when the socket closes (if any).
  flush(): TokenMessage[] {
    const tail = this.buffer.trim();
    this.buffer = "";
    if (tail.length === 0) {
      return [];
    }
    return [parseTokenLine(tail)];
  }
}
