import { assertAtmId } from "./config.js";
import { startAtm, type TxOp } from "./atm.js";

function usage(): never {
  console.error(
    "usage: node dist/main.js --atm <1-4> [--withdraw <n>] [--deposit <n>] ...",
  );
  process.exit(1);
}

function positiveInt(raw: string): number {
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    usage();
  }
  return n;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  let atmId: number | undefined;
  const ops: TxOp[] = [];

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--atm") {
      const value = args[++i];
      if (value === undefined) {
        usage();
      }
      atmId = Number(value);
      continue;
    }
    if (arg === "--withdraw") {
      const value = args[++i];
      if (value === undefined) {
        usage();
      }
      ops.push({ kind: "withdraw", amount: positiveInt(value) });
      continue;
    }
    if (arg === "--deposit") {
      const value = args[++i];
      if (value === undefined) {
        usage();
      }
      ops.push({ kind: "deposit", amount: positiveInt(value) });
      continue;
    }
    usage();
  }

  if (atmId === undefined || !Number.isInteger(atmId)) {
    usage();
  }

  assertAtmId(atmId);
  await startAtm(atmId, ops);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
