# Token Ring Banking

University project for the *Distributed Artificial Intelligence* course,
developed for a course exam. It uses a banking scenario to study distributed
mutual exclusion: separate processes coordinate access to a shared balance
without a central lock. It is a runnable implementation of the Token Ring
algorithm — not production banking software.

Four ATM nodes run as independent OS processes on `localhost`, connected in a
logical ring over TCP. They share a single bank balance. **Mutual exclusion**
is enforced with the classic **Token Ring** algorithm: a token circulates
around the ring, and only the node that currently holds it may read or update
the balance. After processing a queued withdrawal or deposit, the node forwards
the token to its successor with the new balance.

When every node has finished its work, the ring stops automatically. The node
that detects a full lap with no pending transactions injects a `DONE` message;
it travels around the ring and shuts down all four processes in order.

## Requirements

- Node.js 20+
- npm

## Setup

```bash
npm install
npm run build
```

## Running

Recommended — a single command starts all 4 ATMs as separate UNIX
processes and stops them together (manually with `Ctrl+C`, or
automatically when transactions finish):

```bash
npm run demo
```

The script prints the PIDs of the four processes at startup, making it
clear that these are four distinct processes rather than an in-process
simulation inside a single runtime. The scenario run is the one from the
specification example (balance 1000 → 400).

### Manual startup (optional)

Alternatively, each ATM can be launched in a dedicated terminal.
ATM1 injects the token and should be started last.

```bash
# Terminal 2
node dist/main.js --atm 2 --withdraw 200

# Terminal 3
node dist/main.js --atm 3 --deposit 100

# Terminal 4
node dist/main.js --atm 4 --withdraw 500

# Terminal 1
node dist/main.js --atm 1
```

Multiple operations on the same ATM are queued by repeating the options:
`--atm 2 --withdraw 200 --deposit 50`. They are executed one per token
lap.
