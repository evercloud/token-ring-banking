# Token Ring Banking

Quattro processi ATM su localhost, mutua esclusione su un saldo
condiviso tramite Token Ring. Quando tutte le transazioni sono state
eseguite il ring si arresta automaticamente: il primo nodo che vede
un giro completo senza modifiche immette un messaggio `DONE` che fa il
giro e fa uscire ordinatamente i quattro processi.

## Requisiti

- Node.js 20+
- npm

## Setup

```bash
npm install
npm run build
```

## Avvio

Modo consigliato — un singolo comando avvia i 4 ATM come processi
UNIX separati e li ferma tutti insieme (manualmente con `Ctrl+C`,
oppure automaticamente al termine delle transazioni):

```bash
npm run demo
```

Lo script stampa i PID dei quattro processi a inizio esecuzione,
così è evidente che si tratta di quattro processi distinti e non di
una simulazione interna a un singolo runtime. Lo scenario eseguito è
quello dell'esempio della specifica (saldo 1000 → 400).

### Avvio manuale (opzionale)

In alternativa, ogni ATM si può lanciare in un terminale dedicato.
ATM1 inietta il token e conviene avviarlo per ultimo.

```bash
# Terminale 2
node dist/main.js --atm 2 --withdraw 200

# Terminale 3
node dist/main.js --atm 3 --deposit 100

# Terminale 4
node dist/main.js --atm 4 --withdraw 500

# Terminale 1
node dist/main.js --atm 1
```

Più operazioni sullo stesso ATM si accodano ripetendo le opzioni:
`--atm 2 --withdraw 200 --deposit 50`. Vengono eseguite una per giro
del token.
