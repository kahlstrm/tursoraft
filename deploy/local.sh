#!/usr/bin/env bash
# Runs the store on one machine as a stand-in for the regions in
# regions.json: one process per node, with the round trips of
# `local_rtt_ms` injected between nodes by tursoraft's delay table. Then it
# forms the cluster, sets up the store, runs the load against every
# instance at once, and runs the checker.
#
#   deploy/local.sh [--reserve counter|units|leader] [--duration S]
#                   [--flash-at S] [--shoppers N] [--flash-shoppers N]
#                   [--flash-stock N] [--break ack-rejected|oversell]
#                   [--dir DIR] [--keep]
#
# Results and node logs go to DIR (default: a new directory under /tmp).
# --break starts every instance with that deliberate defect, to show that
# the checker catches it. --keep leaves the cluster running afterwards.
# Exits with the checker's status.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(dirname "$here")
regions=$here/regions.json

reserve=counter
duration=60
flash_at=20
shoppers=16
flash_shoppers=8
flash_stock=300
products=100
stock=1000
break_flag=()
dir=
keep=
while [ $# -gt 0 ]; do
  case $1 in
    --reserve) reserve=$2; shift 2 ;;
    --duration) duration=$2; shift 2 ;;
    --flash-at) flash_at=$2; shift 2 ;;
    --shoppers) shoppers=$2; shift 2 ;;
    --flash-shoppers) flash_shoppers=$2; shift 2 ;;
    --flash-stock) flash_stock=$2; shift 2 ;;
    --break) break_flag=(--break "$2"); shift 2 ;;
    --dir) dir=$2; shift 2 ;;
    --keep) keep=1; shift ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
dir=${dir:-$(mktemp -d /tmp/tursoraft-shop.XXXXXX)}
mkdir -p "$dir"

(cd "$root" && cargo build --release -q -p tursoraft-shop)
bin=$root/target/release/tursoraft-shop

raft_base=${RAFT_BASE:-17000}
http_base=${HTTP_BASE:-18000}
admin_base=${ADMIN_BASE:-19000}

# The cluster config: every node on this machine, the regions' round trips
# injected, and the Raft timeouts the real machines use.
jq --argjson base "$raft_base" '
  (.nodes | map({key: .name, value: .id}) | from_entries) as $ids
  | {
      nodes: (.nodes | map({key: (.id | tostring), value: "127.0.0.1:\($base + .id)"}) | from_entries),
      delays: {default_rtt_ms: 0, pairs: (.local_rtt_ms | map({a: $ids[.[0]], b: $ids[.[1]], rtt_ms: .[2]}))},
      timeouts: .timeouts
    }' "$regions" > "$dir/cluster.json"

mapfile -t ids < <(jq -r '.nodes[].id' "$regions")
name() { jq -r --argjson id "$1" '.nodes[] | select(.id == $id) | .name' "$regions"; }
voters=$(jq -c '[.nodes[] | select(.role == "voter") | .id]' "$regions")
followers=$(jq -c '[.nodes[] | select(.role != "voter") | .id]' "$regions")
first_voter=$(jq -r '[.nodes[] | select(.role == "voter") | .id][0]' "$regions")

pids=()
# shellcheck disable=SC2329  # called by the trap
stop() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
if [ -z "$keep" ]; then trap stop EXIT; fi

for id in "${ids[@]}"; do
  n=$(name "$id")
  rm -rf "$dir/node-$id"
  "$bin" serve --config "$dir/cluster.json" --id "$id" --dir "$dir/node-$id" \
    --http "127.0.0.1:$((http_base + id))" --admin "127.0.0.1:$((admin_base + id))" \
    --name "$n" "${break_flag[@]}" > "$dir/node-$id.out" 2> "$dir/node-$id.log" &
  pids+=($!)
done
for id in "${ids[@]}"; do
  for _ in $(seq 100); do grep -q listening "$dir/node-$id.out" 2>/dev/null && break; sleep 0.1; done
  grep -q listening "$dir/node-$id.out" || { echo "node $id did not start:"; cat "$dir/node-$id.log"; exit 1; }
done

admin=http://127.0.0.1:$((admin_base + first_voter))
echo "forming the cluster: voters $voters, non-voting followers $followers"
curl -sf -X POST "$admin/admin/init" -H 'content-type: application/json' \
  -d "{\"voters\": $voters, \"followers\": $followers}" > /dev/null
echo "setting up the store: reserve $reserve, $products products of $stock, flash-sale stock $flash_stock"
index=$(curl -sf -X POST "$admin/admin/setup" -H 'content-type: application/json' \
  -d "{\"reserve\": \"$reserve\", \"products\": $products, \"stock\": $stock, \"flash_stock\": $flash_stock}" | jq .index)
for id in "${ids[@]}"; do
  # A read at the setup's log index waits until the instance has applied it.
  curl -sf "http://127.0.0.1:$((http_base + id))/api/carts/warmup?after=$index" > /dev/null
done

echo "running the load for $duration s on every instance (results in $dir)"
load_pids=()
for id in "${ids[@]}"; do
  n=$(name "$id")
  "$bin" load --url "http://127.0.0.1:$((http_base + id))" --name "$n" --out "$dir/load-$n.json" \
    --duration "$duration" --shoppers "$shoppers" --flash-at "$flash_at" \
    --flash-shoppers "$flash_shoppers" > /dev/null &
  load_pids+=($!)
done
for pid in "${load_pids[@]}"; do wait "$pid"; done

admins=()
for id in "${ids[@]}"; do admins+=(--admin "http://127.0.0.1:$((admin_base + id))"); done
status=0
"$bin" check "${admins[@]}" "$dir"/load-*.json | tee "$dir/report.txt" || status=$?
for id in "${ids[@]}"; do
  echo "node $id: shop http://127.0.0.1:$((http_base + id))  admin http://127.0.0.1:$((admin_base + id))"
done
exit $status
