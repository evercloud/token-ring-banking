import { assertAtmId, ATM_COUNT } from "./config.js";
import { startAtm } from "./atm.js";

const parseAtmId = (): number => {
  const idx = process.argv.indexOf("--atm");
  if (idx === -1 || process.argv[idx + 1] === undefined) {
    console.error(`usage: node dist/main.js --atm <1-${ATM_COUNT}>`);
    process.exit(1);
  }
  const n = Number(process.argv[idx + 1]);
  if (!Number.isInteger(n)) {
    console.error("atm id must be an integer");
    process.exit(1);
  }
  return n;
};

async function main(): Promise<void> {
  const atmId = parseAtmId();
  assertAtmId(atmId);
  await startAtm(atmId);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
