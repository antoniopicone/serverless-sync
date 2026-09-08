#!/usr/bin/env bash
# Scenario di prova. Ogni fase termina con un assert: lo script esce
# diverso da zero alla prima che non passa, quindi e' usabile in CI.
#
#   ./scenario.sh
set -u

A=http://localhost:47101
B=http://localhost:47102
C=http://localhost:47103
LOG=http://localhost:9000
FAILED=0

write() { curl -sS -XPOST "$1/v1/write" -H 'content-type: application/json' \
               -d "{\"entity\":\"$2\",\"value\":\"$3\"}" >/dev/null; }

detail() { curl -sS "$LOG/api/assert" | python3 -c 'import sys,json;print(json.load(sys.stdin)["detail"])'; }

# assert <atteso: converged|diverged> <descrizione> [secondi di attesa]
assert() {
  local want=$1 what=$2 budget=${3:-30} code=""
  for _ in $(seq "$budget"); do
    code=$(curl -sS -o /dev/null -w '%{http_code}' "$LOG/api/assert")
    if   [ "$want" = converged ] && [ "$code" = 200 ]; then break
    elif [ "$want" = diverged  ] && [ "$code" = 409 ]; then break; fi
    sleep 1
  done
  if { [ "$want" = converged ] && [ "$code" = 200 ]; } || \
     { [ "$want" = diverged  ] && [ "$code" = 409 ]; }; then
    printf '  ok    %-46s %s\n' "$what" "$(detail)"
  else
    printf '  FALLITO %-44s %s\n' "$what" "$(detail)"; FAILED=1
  fi
}

echo "== attendo che i nodi si trovino sulla tailnet =="
for _ in $(seq 60); do
  curl -sf "$A/v1/node" >/dev/null 2>&1 && break; sleep 2
done
for u in "$A" "$B" "$C"; do
  curl -sf "$u/v1/node" >/dev/null || { echo "  $u non risponde"; exit 1; }
done
# nessun --bootstrap: se qui si vedono, la discovery via tailscaled funziona
sleep 8
echo "  peer visti da device-a:"
curl -sS -XPOST "$A/v1/peers" -H 'content-type: application/json' -d '{}' \
  | python3 -c 'import sys,json;[print("   ",p["hostname"],p["addr"]) for p in json.load(sys.stdin)]'

echo; echo "== 1. scritture concorrenti su nodi diversi =="
write "$A" github  segreto-da-A
write "$C" gitlab  segreto-da-C
assert converged "tutti e tre allineati"

echo; echo "== 2. partizione: device-c sospeso =="
docker compose pause device-c >/dev/null
assert diverged "il banco segnala un nodo assente" 20

echo; echo "== 3. conflitto sulla stessa voce durante la partizione =="
write "$A" github CONFLITTO-da-A
# device-c e' in pausa: la sua scrittura arrivera' solo al risveglio
assert diverged "ancora partizionato" 10

echo; echo "== 4. guarigione =="
docker compose unpause device-c >/dev/null
assert converged "convergono senza intervento" 45
echo "  vincitore su github:"
curl -sS "$A/v1/state" | python3 -c '
import sys,json;d=json.load(sys.stdin)
print("   ", [e for e in d["entries"] if e["entity"]=="github"][0]["value"])'

echo; echo "== 5. il logger non e\x27 una dipendenza =="
docker compose stop logger >/dev/null
write "$B" senza-logger ok
sleep 10
FP=$(curl -sS "$A/v1/state" | python3 -c 'import sys,json;print(json.load(sys.stdin)["fingerprint"])')
SAME=1
for u in "$B" "$C"; do
  f=$(curl -sS "$u/v1/state" | python3 -c 'import sys,json;print(json.load(sys.stdin)["fingerprint"])')
  [ "$f" = "$FP" ] || SAME=0
done
docker compose start logger >/dev/null
if [ "$SAME" = 1 ]; then
  printf '  ok    %-46s %s\n' "i nodi convergono col logger spento" "$FP"
else
  printf '  FALLITO %-44s\n' "i nodi hanno divergito col logger spento"; FAILED=1
fi

echo
if [ "$FAILED" = 0 ]; then echo "Tutte le fasi superate."; else echo "Almeno una fase fallita."; fi
exit $FAILED
