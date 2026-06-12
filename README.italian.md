# Token Ring Banking

Progetto universitario per il corso di *Intelligenza Artificiale Distribuita*,
sviluppato per un esame. Usa uno scenario bancario per studiare la mutua
esclusione distribuita: processi separati coordinano l'accesso a un saldo
condiviso senza un lock centralizzato. È un'implementazione eseguibile
dell'algoritmo Token Ring — non un sistema bancario di produzione.

Quattro nodi ATM girano come processi indipendenti del sistema operativo su
`localhost`, collegati in un anello logico via TCP. Condividono un unico saldo
bancario. La **mutua esclusione** è garantita dall'algoritmo classico **Token
Ring**: un token circola sull'anello e solo il nodo che lo possiede può
leggere o aggiornare il saldo. Dopo aver elaborato un prelievo o un deposito in
coda, il nodo inoltra il token al successore con il saldo aggiornato.

Quando ogni nodo ha terminato il proprio lavoro, il ring si arresta
automaticamente. Il nodo che rileva un giro completo senza transazioni in
sospeso immette un messaggio `DONE` che percorre l'anello e fa uscire
ordinatamente tutti e quattro i processi.

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
