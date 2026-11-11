#!/usr/bin/env bash
# The provider-specific part: creates and deletes the machines of
# regions.json on Hetzner Cloud with the hcloud CLI. Everything after the
# machines exist is in cluster.sh, which only needs SSH as root.
#
#   deploy/hetzner.sh plan          what `up` would create, and its hourly price
#   deploy/hetzner.sh up [--yes]    create the SSH key, the firewall and one machine per node
#   deploy/hetzner.sh list          the machines, their age and what they cost so far
#   deploy/hetzner.sh down [--yes]  delete every machine, the firewall and the SSH key
#
# Needs HCLOUD_TOKEN (a read-write API token of the project) in the
# environment. Everything it creates carries the label tursoraft-shop=1, and
# `down` deletes exactly what carries it. `up` writes state/hosts, one line
# per machine: node id, name, role, public IPv4. cluster.sh reads it.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
regions=$here/regions.json
state=$here/state
label=tursoraft-shop=1
prefix=tursoraft-shop

die() { echo "$*" >&2; exit 1; }
need_token() {
  command -v hcloud > /dev/null || die "hcloud is not installed: https://github.com/hetznercloud/cli/releases"
  [ -n "${HCLOUD_TOKEN:-}" ] || die "HCLOUD_TOKEN is not set: create a read-write API token in the Hetzner Cloud project and export it"
}
confirm() {
  [ "${yes:-}" = 1 ] && return
  read -r -p "$1 [y/N] " answer
  [ "$answer" = y ] || die "stopped"
}

type=$(jq -r .server_type "$regions")
image=$(jq -r .image "$regions")

# Prints the machines `up` creates, and with a token, each one's hourly price
# before VAT from the API. Fails if a location does not offer the type.
plan() {
  echo "machines to create (type $type, image $image, label $label):"
  local prices=
  if [ -n "${HCLOUD_TOKEN:-}" ]; then
    prices=$(hcloud server-type describe "$type" -o json | jq -c '[.prices[] | {location, net: (.price_hourly.net | tonumber), gross: (.price_hourly.gross | tonumber)}]')
  fi
  local total=0 missing=
  while read -r id name location role; do
    local price=
    if [ -n "$prices" ]; then
      price=$(jq -r --arg l "$location" '.[] | select(.location == $l) | .net' <<< "$prices")
      if [ -z "$price" ]; then
        missing="$missing $location"
        price="not offered here"
      else
        total=$(jq -n "$total + $price")
        price=$(printf 'EUR %.4f/h before VAT' "$price")
      fi
    fi
    printf '  %-20s %-6s %-9s %s\n' "$prefix-$name" "$location" "$role" "${price:-price needs HCLOUD_TOKEN}"
  done < <(jq -r '.nodes[] | "\(.id) \(.name) \(.location) \(.role)"' "$regions")
  echo "  plus one firewall (SSH, WireGuard UDP $(jq .wireguard_port "$regions"), HTTP TCP $(jq .http_port "$regions")) and one SSH key, both free"
  if [ -n "$prices" ]; then
    printf '  total: EUR %.4f per hour before VAT, EUR %.2f per day\n' "$total" "$(jq -n "$total * 24")"
    [ -z "$missing" ] || die "type $type is not offered in:$missing"
  fi
}

up() {
  need_token
  plan
  [ -z "$(hcloud server list -l "$label" -o noheader -o columns=name)" ] \
    || die "machines with label $label exist already; run 'down' first or use them"
  confirm "create these machines?"
  mkdir -p "$state"
  [ -f "$state/id_ed25519" ] || ssh-keygen -q -t ed25519 -N '' -C "$prefix" -f "$state/id_ed25519"
  hcloud ssh-key describe "$prefix" > /dev/null 2>&1 \
    || hcloud ssh-key create --name "$prefix" --label "$label" --public-key-from-file "$state/id_ed25519.pub" > /dev/null
  if ! hcloud firewall describe "$prefix" > /dev/null 2>&1; then
    jq '[
      {direction: "in", protocol: "tcp", port: "22", source_ips: ["0.0.0.0/0", "::/0"], description: "ssh"},
      {direction: "in", protocol: "udp", port: (.wireguard_port | tostring), source_ips: ["0.0.0.0/0", "::/0"], description: "wireguard"},
      {direction: "in", protocol: "tcp", port: (.http_port | tostring), source_ips: ["0.0.0.0/0", "::/0"], description: "shop"}
    ]' "$regions" > "$state/firewall.json"
    hcloud firewall create --name "$prefix" --label "$label" --rules-file "$state/firewall.json" > /dev/null
  fi
  local pids=()
  while read -r id name location role; do
    hcloud server create --name "$prefix-$name" --type "$type" --image "$image" --location "$location" \
      --ssh-key "$prefix" --firewall "$prefix" --label "$label" --label "node=$id" --label "role=$role" \
      > "$state/create-$name.log" 2>&1 &
    pids+=($!)
  done < <(jq -r '.nodes[] | "\(.id) \(.name) \(.location) \(.role)"' "$regions")
  local failed=0
  for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
  hosts
  [ "$failed" = 0 ] || die "creating a machine failed; see $state/create-*.log, and run 'down' to delete what was created"
  echo "created; next: deploy/cluster.sh install"
}

# Writes state/hosts from the machines that exist.
hosts() {
  hcloud server list -l "$label" -o json | jq -r --slurpfile r "$regions" '
    .[] | (.name | sub("^tursoraft-shop-"; "")) as $name
    | ($r[0].nodes[] | select(.name == $name)) as $n
    | "\($n.id) \($name) \($n.role) \(.public_net.ipv4.ip)"' | sort -n > "$state/hosts"
  cat "$state/hosts"
}

list() {
  need_token
  local prices
  prices=$(hcloud server-type describe "$type" -o json | jq -c '[.prices[] | {location, net: (.price_hourly.net | tonumber)}]')
  hcloud server list -l "$label" -o json | jq -r --argjson p "$prices" '
    (now) as $now
    | .[] | (.location.name) as $l
    | (($now - (.created | sub("\\+00:00$"; "Z") | fromdateiso8601)) / 3600) as $h
    | ([$p[] | select(.location == $l) | .net][0]) as $price
    | "\(.name)  \($l)  \(.status)  \(.public_net.ipv4.ip)  \($h * 100 | floor / 100) h  about EUR \(($h | ceil) * $price * 100 | ceil / 100)"'
  echo "(Hetzner bills each started hour; the sum above rounds up per machine)"
}

down() {
  need_token
  local servers
  servers=$(hcloud server list -l "$label" -o noheader -o columns=name)
  servers=${servers//$'\n'/ }
  echo "to delete: ${servers:-no machines}; firewall $prefix; SSH key $prefix"
  confirm "delete these?"
  # shellcheck disable=SC2086
  [ -z "$servers" ] || hcloud server delete $servers
  hcloud firewall delete "$prefix" 2> /dev/null || true
  hcloud ssh-key delete "$prefix" 2> /dev/null || true
  rm -f "$state/hosts"
  [ -z "$(hcloud server list -l "$label" -o noheader -o columns=name)" ] || die "machines with label $label remain"
  echo "deleted"
}

cmd=${1:-}
[ "${2:-}" = --yes ] && yes=1
case $cmd in
  plan) plan ;;
  up) up ;;
  list) list ;;
  hosts) need_token; mkdir -p "$state"; hosts ;;
  down) down ;;
  *) die "usage: $0 plan|up|list|hosts|down [--yes]" ;;
esac
