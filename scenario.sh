#!/usr/bin/env bash
# Test scenario. Every phase ends with an assertion: the script exits
# non-zero at the first one that fails, so it's usable in CI.
#
#   ./scenario.sh
set -u

A=http://localhost:47101
B=http://localhost:47102
C=http://localhost:47103
LOG=http://localhost:9000
FAILED=0

# This rig's own demo application (see docker-compose.yml's
# SERVICE_NAME/SERVICE_TOKEN, and "Registering an application" in
# README.md) — every per-application syncd endpoint is namespaced under it.
SVC=demo/v1

write() { curl -sS -XPOST "$1/v1/$SVC/write" -H 'content-type: application/json' \
               -d "{\"entity\":\"$2\",\"value\":\"$3\"}" >/dev/null; }
state() { curl -sS "$1/v1/$SVC/state"; }

detail() { curl -sS "$LOG/api/assert" | python3 -c 'import sys,json;print(json.load(sys.stdin)["detail"])'; }

# assert <expected: converged|diverged> <description> [seconds to wait]
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
    printf '  FAILED %-44s %s\n' "$what" "$(detail)"; FAILED=1
  fi
}

echo "== waiting for nodes to find each other on the tailnet =="
for _ in $(seq 60); do
  curl -sf "$A/v1/node" >/dev/null 2>&1 && break; sleep 2
done
for u in "$A" "$B" "$C"; do
  curl -sf "$u/v1/node" >/dev/null || { echo "  $u not responding"; exit 1; }
done
# no --bootstrap: if they see each other here, discovery via tailscaled works
sleep 8
echo "  peers seen by device-a:"
curl -sS -XPOST "$A/v1/peers" -H 'content-type: application/json' -d '{}' \
  | python3 -c 'import sys,json;[print("   ",p["hostname"],p["addr"]) for p in json.load(sys.stdin)]'

echo; echo "== 1. concurrent writes on different nodes =="
write "$A" github  secret-from-A
write "$C" gitlab  secret-from-C
assert converged "all three aligned"

echo; echo "== 2. partition: device-c suspended =="
docker compose pause device-c >/dev/null
assert diverged "the rig flags a missing node" 20

echo; echo "== 3. conflict on the same entry during the partition =="
write "$A" github CONFLICT-from-A
# device-c is paused: its write will only arrive once it wakes up
assert diverged "still partitioned" 10

echo; echo "== 4. recovery =="
docker compose unpause device-c >/dev/null
assert converged "converges with no intervention" 45
echo "  winner on github:"
state "$A" | python3 -c '
import sys,json;d=json.load(sys.stdin)
print("   ", [e for e in d["entries"] if e["entity"]=="github"][0]["value"])'

echo; echo "== 5. the logger is not a dependency =="
docker compose stop logger >/dev/null
write "$B" without-logger ok
sleep 10
FP=$(state "$A" | python3 -c 'import sys,json;print(json.load(sys.stdin)["fingerprint"])')
SAME=1
for u in "$B" "$C"; do
  f=$(state "$u" | python3 -c 'import sys,json;print(json.load(sys.stdin)["fingerprint"])')
  [ "$f" = "$FP" ] || SAME=0
done
docker compose start logger >/dev/null
if [ "$SAME" = 1 ]; then
  printf '  ok    %-46s %s\n' "nodes converge with the logger off" "$FP"
else
  printf '  FAILED %-44s\n' "nodes diverged with the logger off"; FAILED=1
fi

echo
if [ "$FAILED" = 0 ]; then echo "All phases passed."; else echo "At least one phase failed."; fi
exit $FAILED
