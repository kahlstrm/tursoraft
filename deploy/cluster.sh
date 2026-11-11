#!/usr/bin/env bash
# shellcheck disable=SC2016,SC2029  # remote commands are built on purpose
# Everything after the machines exist, on any provider: needs only SSH as
# root to each machine listed in state/hosts (one line per machine: node id,
# name, role, public IPv4), as hetzner.sh writes it, and the SSH key
# state/id_ed25519.
#
#   deploy/cluster.sh install [BINARY]   WireGuard between the machines, the store as a service
#   deploy/cluster.sh rtt                round trips between the machines over WireGuard
#   deploy/cluster.sh init [--reserve counter|units|leader] [--products N] [--stock N] [--flash-stock N]
#                                        form the cluster and set up the store
#   deploy/cluster.sh load [LOAD ARGS]   run the load on every machine against its own
#                                        instance at once, collect the results, run the checker
#   deploy/cluster.sh status             every instance's view of the cluster
#   deploy/cluster.sh reset              stop every instance and delete its data, to start over
#   deploy/cluster.sh edge --worker URL --on NAME=FOLLOWER [--on NAME=FOLLOWER ...]
#                                        from the next install, the instance on machine NAME
#                                        relays its changes to follower FOLLOWER of the edge
#                                        Worker at URL (edge/); --off stops every relay
#
# The nodes talk over WireGuard (10.77.0.ID), because tursoraft's traffic has
# no encryption and no authentication. The shop's HTTP port is public; the
# admin port listens on the WireGuard address only. On a real network the
# delay table stays empty and the Raft timeouts come from regions.json.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(dirname "$here")
regions=$here/regions.json
state=${DEPLOY_STATE:-$here/state}
hosts=$state/hosts

die() { echo "$*" >&2; exit 1; }
[ -f "$hosts" ] || die "no $hosts: create the machines first (deploy/hetzner.sh up)"

net=$(jq -r .wireguard_net "$regions")
wg_port=$(jq -r .wireguard_port "$regions")
raft_port=$(jq -r .raft_port "$regions")
http_port=$(jq -r .http_port "$regions")
admin_port=$(jq -r .admin_port "$regions")

mapfile -t lines < "$hosts"
field() { awk -v n="$2" '{print $n}' <<< "$1"; }

# The edge tier, if `edge` turned it on: state/edge holds the Worker's URL
# and which machine relays to which follower, state/relay-secret the
# secret the relays and the Worker share.
edge=$state/edge
secret=$state/relay-secret
worker() { awk '$1 == "worker" {print $2}' "$edge"; }
# The follower the instance on machine $1 relays to, or nothing.
follower_of() { [ ! -f "$edge" ] || awk -v n="$1" '$1 == "relay" && $2 == n {print $3}' "$edge"; }
first_voter=$(awk '$3 == "voter" {print; exit}' "$hosts")

ssh_opts=(-i "$state/id_ed25519" -o StrictHostKeyChecking=accept-new
  -o UserKnownHostsFile="$state/known_hosts" -o ConnectTimeout=10 -o BatchMode=yes)
on() { local ip=$1; shift; ssh "${ssh_opts[@]}" "root@$ip" "$@"; }
copy() { scp -q "${ssh_opts[@]}" "$@"; }

# Runs `$1 LINE` for every host at once; fails if any fails. Output goes to
# state/logs/STEP-NAME.log.
each() {
  local step=$1 pids=() names=() failed=()
  mkdir -p "$state/logs"
  for line in "${lines[@]}"; do
    local name; name=$(field "$line" 2)
    "$step" "$line" > "$state/logs/$step-$name.log" 2>&1 &
    pids+=($!); names+=("$name")
  done
  for i in "${!pids[@]}"; do
    wait "${pids[$i]}" || failed+=("${names[$i]}")
  done
  [ ${#failed[@]} = 0 ] || die "$step failed on ${failed[*]}; see $state/logs/$step-*.log"
}

# Installs WireGuard and makes the machine's key; prints its public key.
prepare() {
  local ip; ip=$(field "$1" 4)
  on "$ip" 'set -e
    export DEBIAN_FRONTEND=noninteractive
    command -v wg > /dev/null || { apt-get update -q; apt-get install -yq wireguard-tools curl jq; }
    umask 077
    [ -f /etc/wireguard/private.key ] || wg genkey > /etc/wireguard/private.key
    wg pubkey < /etc/wireguard/private.key' | tail -n 1 > "$state/pubkeys/$(field "$1" 2)"
}

# Writes the files of every machine under state/render/NAME.
render() {
  rm -rf "$state/render"
  jq --arg net "$net" --argjson port "$raft_port" --rawfile hosts "$hosts" '{
      nodes: ($hosts | split("\n") | map(select(. != "") | split(" ")) | map({key: .[0], value: "\($net).\(.[0]):\($port)"}) | from_entries),
      delays: {default_rtt_ms: 0, pairs: []},
      timeouts: .timeouts
    }' "$regions" > "$state/cluster.json"
  for line in "${lines[@]}"; do
    local id name dir
    id=$(field "$line" 1); name=$(field "$line" 2); dir=$state/render/$name
    mkdir -p "$dir"
    cp "$state/cluster.json" "$dir/cluster.json"
    {
      echo "[Interface]"
      echo "Address = $net.$id/24"
      echo "ListenPort = $wg_port"
      echo "PostUp = wg set %i private-key /etc/wireguard/private.key"
      for peer in "${lines[@]}"; do
        local pid pname; pid=$(field "$peer" 1); pname=$(field "$peer" 2)
        [ "$pid" = "$id" ] && continue
        echo
        echo "# $pname"
        echo "[Peer]"
        echo "PublicKey = $(cat "$state/pubkeys/$pname")"
        echo "Endpoint = $(field "$peer" 4):$wg_port"
        echo "AllowedIPs = $net.$pid/32"
        echo "PersistentKeepalive = 25"
      done
    } > "$dir/wg0.conf"
    # A relaying instance gets the secret as a systemd credential: the unit
    # names only the file, and the service user reads its own copy of it.
    local follower relay_args="" credential=""
    follower=$(follower_of "$name")
    if [ -n "$follower" ]; then
      relay_args=" --relay $(worker | sed 's|^http|ws|')/relay?follower=$follower --relay-secret-file \${CREDENTIALS_DIRECTORY}/relay-secret"
      credential=$'\n'"LoadCredential=relay-secret:/etc/tursoraft-shop/relay-secret"
    fi
    cat > "$dir/tursoraft-shop.service" <<EOF
[Unit]
Description=tursoraft shop, node $id ($name)
Requires=wg-quick@wg0.service
After=wg-quick@wg0.service network-online.target

[Service]
ExecStart=/usr/local/bin/tursoraft-shop serve --config /etc/tursoraft-shop/cluster.json --id $id --dir /var/lib/tursoraft-shop --http 0.0.0.0:$http_port --admin $net.$id:$admin_port --name $name$relay_args$credential
DynamicUser=yes
StateDirectory=tursoraft-shop
Environment=RUST_LOG=warn
LimitNOFILE=65536
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF
  done
}

upload() {
  local name ip dir; name=$(field "$1" 2); ip=$(field "$1" 4); dir=$state/render/$name
  on "$ip" 'mkdir -p /etc/tursoraft-shop; systemctl stop tursoraft-shop 2> /dev/null || true'
  copy "$bin" "root@$ip:/usr/local/bin/tursoraft-shop"
  copy "$dir/wg0.conf" "root@$ip:/etc/wireguard/wg0.conf"
  copy "$dir/cluster.json" "root@$ip:/etc/tursoraft-shop/cluster.json"
  copy "$dir/tursoraft-shop.service" "root@$ip:/etc/systemd/system/tursoraft-shop.service"
  # Over SSH's stdin, so the secret is never in a command line, and
  # readable by root only from the start.
  if [ -n "$(follower_of "$name")" ]; then
    on "$ip" 'umask 077; cat > /etc/tursoraft-shop/relay-secret' < "$secret"
  else
    on "$ip" 'rm -f /etc/tursoraft-shop/relay-secret'
  fi
  on "$ip" 'set -e
    chmod 600 /etc/wireguard/wg0.conf
    systemctl daemon-reload
    systemctl enable -q wg-quick@wg0 tursoraft-shop
    systemctl restart wg-quick@wg0
    systemctl restart tursoraft-shop
    for _ in $(seq 50); do curl -sf http://127.0.0.1:'"$http_port"'/api/status && exit 0; sleep 0.2; done
    journalctl -u tursoraft-shop -n 50 --no-pager; exit 1'
}

install() {
  bin=${1:-}
  if [ -z "$bin" ]; then
    (cd "$root" && cargo build --release -q -p tursoraft-shop)
    bin=$root/target/release/tursoraft-shop
  fi
  mkdir -p "$state/pubkeys"
  echo "installing WireGuard on ${#lines[@]} machines"
  each prepare
  render
  echo "installing $bin as the service tursoraft-shop"
  each upload
  status
}

rtt() {
  printf '%-6s' ""
  for peer in "${lines[@]}"; do printf '%9s' "$(field "$peer" 2)"; done
  echo "   (median of 5 pings over WireGuard, ms)"
  for line in "${lines[@]}"; do
    local targets=""
    for peer in "${lines[@]}"; do targets="$targets $net.$(field "$peer" 1)"; done
    printf '%-6s' "$(field "$line" 2)"
    # shellcheck disable=SC2016
    on "$(field "$line" 4)" 'for t in '"$targets"'; do
        ping -q -c 5 -i 0.2 -W 2 "$t" | awk -F/ "/^rtt/ {printf \"%9.1f\", \$5; f=1} END {if (!f) printf \"%9s\", \"-\"}"
      done; echo'
  done
}

init() {
  local reserve=counter products=100 stock=1000 flash_stock=300
  while [ $# -gt 0 ]; do
    case $1 in
      --reserve) reserve=$2; shift 2 ;;
      --products) products=$2; shift 2 ;;
      --stock) stock=$2; shift 2 ;;
      --flash-stock) flash_stock=$2; shift 2 ;;
      *) die "unknown argument $1" ;;
    esac
  done
  local voters followers ip admin
  voters=$(awk '$3 == "voter" {print $1}' "$hosts" | jq -sc .)
  followers=$(awk '$3 != "voter" {print $1}' "$hosts" | jq -sc .)
  ip=$(field "$first_voter" 4)
  admin=http://$net.$(field "$first_voter" 1):$admin_port
  echo "forming the cluster: voters $voters, non-voting followers $followers"
  on "$ip" "curl -sSf -X POST $admin/admin/init -H 'content-type: application/json' \
    -d '{\"voters\": $voters, \"followers\": $followers}' > /dev/null"
  echo "setting up the store: reserve $reserve, $products products of $stock, flash-sale stock $flash_stock"
  on "$ip" "curl -sSf -X POST $admin/admin/setup -H 'content-type: application/json' \
    -d '{\"reserve\": \"$reserve\", \"products\": $products, \"stock\": $stock, \"flash_stock\": $flash_stock}'"
  echo
  status
}

load_one() {
  local name ip; name=$(field "$1" 2); ip=$(field "$1" 4)
  # shellcheck disable=SC2086
  on "$ip" "tursoraft-shop load --url http://127.0.0.1:$http_port --name $name --out /root/load-$name.json $load_args"
  copy "root@$ip:/root/load-$name.json" "$run/"
}

load() {
  load_args="$*"
  run=$state/results/$(date -u +%Y%m%dT%H%M%SZ)
  mkdir -p "$run"
  echo "running the load on every machine at once ($load_args); results in $run"
  each load_one
  local ip admins=""
  ip=$(field "$first_voter" 4)
  for line in "${lines[@]}"; do admins="$admins --admin http://$net.$(field "$line" 1):$admin_port"; done
  copy "$run"/load-*.json "root@$ip:/root/"
  local status=0
  on "$ip" "tursoraft-shop check $admins /root/load-*.json" | tee "$run/report.txt" || status=$?
  exit $status
}

status() {
  for line in "${lines[@]}"; do
    printf '%-4s http://%s:%s  ' "$(field "$line" 2)" "$(field "$line" 4)" "$http_port"
    curl -sS --max-time 5 "http://$(field "$line" 4):$http_port/api/status" || printf 'no answer'
    echo
  done
  [ -f "$edge" ] || return 0
  for line in "${lines[@]}"; do
    local f; f=$(follower_of "$(field "$line" 2)")
    [ -n "$f" ] && echo "$(field "$line" 2) relays to $(worker)/?follower=$f"
  done
  return 0
}

edge_cmd() {
  local url="" relays=() off=""
  while [ $# -gt 0 ]; do
    case $1 in
      --worker) url=${2%/}; shift 2 ;;
      --on) relays+=("$2"); shift 2 ;;
      --off) off=1; shift ;;
      *) die "unknown argument $1" ;;
    esac
  done
  if [ -n "$off" ]; then
    rm -f "$edge"
    echo "no instance relays from the next install"
    return
  fi
  [[ $url =~ ^https?://[A-Za-z0-9.:-]+$ ]] || die "--worker is the Worker's base URL, such as https://tursoraft-edge.NAME.workers.dev"
  [ ${#relays[@]} -gt 0 ] || die "name at least one --on MACHINE=FOLLOWER"
  local out="worker $url" seen=" " followers=" " relay name follower
  for relay in "${relays[@]}"; do
    name=${relay%%=*}; follower=${relay#*=}
    awk -v n="$name" '$2 == n {f=1} END {exit !f}' "$hosts" || die "no machine $name in $hosts"
    [[ $follower =~ ^[A-Za-z0-9_-]+$ ]] || die "a follower name is letters, digits, '-' or '_', not '$follower'"
    # Two relays to one follower would take it over from each other.
    [[ $seen == *" $name "* ]] && die "machine $name is named twice"
    [[ $followers == *" $follower "* ]] && die "follower $follower has two relays"
    seen="$seen$name "; followers="$followers$follower "
    out="$out"$'\n'"relay $name $follower"
  done
  echo "$out" > "$edge"
  if [ ! -s "$secret" ]; then
    (umask 077; head -c 32 /dev/urandom | base64 | tr -d '+/=\n' > "$secret")
    echo "made a new secret in $secret"
  fi
  echo "relays from the next install:"
  sed -n 's/^relay \(.*\) \(.*\)/  \1 -> \2/p' "$edge"
  echo "the Worker needs the same secret: (cd edge && npx wrangler secret put RELAY_SECRET < $secret)"
  echo "next: deploy/cluster.sh install"
}

# A follower refuses a copy older than what it has, and a wiped cluster
# starts again at log index 1: make each follower forget its copy.
forget_followers() {
  [ -f "$edge" ] || return 0
  local header f
  header=$(mktemp "$state/header.XXXXXX")
  printf 'Authorization: Bearer %s\n' "$(cat "$secret")" > "$header"
  while read -r f; do
    curl -sSf --max-time 30 -X POST -H @"$header" "$(worker)/admin/forget?follower=$f" > /dev/null \
      || echo "follower $f did not forget its copy: the relay will be refused until it does" >&2
  done < <(awk '$1 == "relay" {print $3}' "$edge")
  rm -f "$header"
}

# Every node is stopped and wiped before any starts again: a node that
# still runs the old cluster would connect to a wiped one, and nodes refuse
# connections meant for an earlier incarnation of their directory.
wipe_one() {
  on "$(field "$1" 4)" 'systemctl stop tursoraft-shop; rm -rf /var/lib/private/tursoraft-shop /var/lib/tursoraft-shop'
}

start_one() {
  on "$(field "$1" 4)" 'systemctl start tursoraft-shop'
}

cmd=${1:-}
shift || true
case $cmd in
  install) install "$@" ;;
  render) render ;;
  rtt) rtt ;;
  init) init "$@" ;;
  load) load "$@" ;;
  status) status ;;
  reset) each wipe_one; forget_followers; each start_one; echo "every instance is empty; next: init" ;;
  edge) edge_cmd "$@" ;;
  *) die "usage: $0 install [BINARY] | rtt | init [...] | load [...] | status | reset | edge [...]" ;;
esac
