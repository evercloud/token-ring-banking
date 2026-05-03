#!/usr/bin/env bash
# Avvia i 4 nodi ATM come processi UNIX separati e li ferma tutti
# insieme con un singolo Ctrl+C. I PID stampati a inizio esecuzione
# sono la prova che si tratta di quattro processi distinti.

set -e

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

# Build se manca, così lo script funziona anche da clean checkout.
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
# Piccola pausa: ATM1 inietta il token, partiamo per ultimi così
# i listener degli altri sono sicuramente attivi.
sleep 0.5
start_atm ATM1 --atm 1
echo "[demo] ring active, Ctrl+C to stop all."
echo

wait
