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

// Wire predecessor socket, successor socket, forward TOKEN lines (balance unchanged for now).
export const startAtm = async (atmId: number): Promise<void> => {
  const port = listenPort(atmId);
  const succ = successorPort(atmId);

  const incomingP = acceptFirstConnection(port);
  const outgoingP = connectWithRetry(succ);
  const [fromPred, toSucc] = await Promise.all([incomingP, outgoingP]);

  const decoder = new TokenLineDecoder();

  const forward = (msg: TokenMessage): void => {
    toSucc.write(serializeTokenMessage(msg.balance));
  };

  fromPred.on("data", (buf: Buffer) => {
    const msgs = decoder.append(buf.toString("utf8"));
    for (const msg of msgs) {
      forward(msg);
    }
  });

  fromPred.on("end", () => {
    for (const msg of decoder.flush()) {
      forward(msg);
    }
  });

  if (atmId === 1) {
    toSucc.write(serializeTokenMessage(INITIAL_BALANCE));
  }
};
