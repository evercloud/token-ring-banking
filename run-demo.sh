#!/usr/bin/env bash
# Start the 4 ATM nodes as separate UNIX processes and stop them all
# together with a single Ctrl+C. PIDs printed at startup prove these
# are four distinct processes.

set -e

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

# Build if missing, so the script works from a clean checkout too.
if [ ! -f dist/main.js ]; then
  echo "[demo] dist/ missing, running npm run build..."
  npm run build
  echo
fi

pids=()

cleanup() {
  echo
  echo "[demo] shutting down..."
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  echo "[demo] all ATM processes terminated."
}
trap cleanup INT TERM

start_atm() {
  local label=$1
  shift
  node dist/main.js "$@" &
  local pid=$!
  pids+=("$pid")
  printf "[demo]   %s -> PID %d\n" "$label" "$pid"
}

echo "[demo] starting 4 ATMs as separate UNIX processes:"
start_atm ATM2 --atm 2 --withdraw 200
start_atm ATM3 --atm 3 --deposit 100
start_atm ATM4 --atm 4 --withdraw 500
# Short pause: ATM1 injects the token, start it last so the other
# nodes' listeners are definitely up.
sleep 0.5
start_atm ATM1 --atm 1
echo "[demo] ring active, Ctrl+C to stop all."
echo

wait
