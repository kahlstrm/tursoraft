#!/usr/bin/env python3
"""Turns the result rows of tursoraft-raft-bench into the Markdown tables of
the phase 1c report.

    python3 -I crates/raft/bench/report.py crates/raft/bench-results/phase1c.jsonl

A job that was run more than once is reported from its last row; the earlier
rows stay in the file.
"""

import json
import sys


def main(path):
    rows = {}
    for line in open(path):
        line = line.strip()
        if line:
            r = json.loads(line)
            rows[r["name"]] = r
    errors = [r for r in rows.values() if "error" in r]
    out = []
    p = out.append
    fs = fsync_table(rows, p)
    p("")
    cells = [r for r in rows.values() if r["kind"] == "cell" and "result" in r]
    for group, title in [
        ("voters_x_rtt", "Voters × round trip (1 writer, batching off, 200 B, at the leader)"),
        ("writers_x_batching_x_rtt", "Writers × batching × round trip (3 voters, 200 B, at the leader)"),
        ("size", "Proposal size (3 voters, batching on, at the leader)"),
        ("learners", "Non-voting followers (3 voters, 200 B, batching on, at the leader)"),
        ("issue_at", "Where the write is issued (3 voters, 200 B, batching on)"),
        ("batching_in_flight", "Batcher limit on entries in flight (3 voters, 200 B)"),
        ("proxy", "Delay in the node's network layer vs. a TCP proxy (3 voters, 200 B, batching on)"),
    ]:
        sel = [r for r in cells if r["group"] == group]
        if group == "writers_x_batching_x_rtt":
            sel += [
                r for r in cells
                if r["group"] == "voters_x_rtt" and len(r["voters"]) == 3
            ]
        if not sel:
            continue
        p(f"### {title}")
        p("")
        cell_table(sel, fs, p)
        p("")
    aws = [r for r in cells if r["group"].startswith("aws_")]
    if aws:
        p("### AWS regions (cloudping.co P50; leader in Frankfurt, 200 B, batching on)")
        p("")
        cell_table(aws, fs, p, aws=True)
        p("")
    group_commit_table(cells, p)
    learner_lag_table(cells, p)
    failover_table(rows, p)
    transfer_table(rows, p)
    if errors:
        p("### Jobs that failed")
        p("")
        for r in errors:
            p(f"- `{r['name']}`: {r['error']}")
    print("\n".join(out))


def fsync_table(rows, p):
    fs = [r for r in rows.values() if r["kind"] == "fsync" and "result" in r]
    base = None
    if not fs:
        return 0.0
    p("### fdatasync of this orb (append a record, then fdatasync)")
    p("")
    p("| record | threads | samples | p50 ms | p99 ms |")
    p("|---|---|---|---|---|")
    for r in fs:
        l = r["result"]["latency_ms"]
        p(f"| {r['size']} B | {r['parallel']} | {r['result']['samples']} | {l['p50']:.2f} | {l['p99']:.2f} |")
        if r["size"] == 200 and r["parallel"] == 1:
            base = l["p50"]
    return base or 0.0


def rtt_label(r):
    if r.get("proxy_rtt_ms") is not None:
        return f"{r['proxy_rtt_ms']:g} (proxy)"
    if r.get("regions"):
        return r["group"]
    return f"{r['delays']['default_rtt_ms']:g}"


def cell_table(sel, fsync_ms, p, aws=False):
    def key(r):
        return (
            r["group"],
            r.get("proxy_rtt_ms") or r["delays"]["default_rtt_ms"],
            len(r["voters"]),
            len(r["learners"]),
            r["size"],
            r["writers"],
            r["batching"],
            r["issue_at"],
            r.get("proxy_rtt_ms") is not None,
            r.get("max_entries_in_flight") or 2,
        )

    p("| RTT ms | voters | non-voting | size | writers | batching | in flight | at | quorum RTT ms | p50 ms | p99 ms | p50 − quorum RTT | writes/s | entries/write | voter AppendEntries with entries per write | samples | window s |")
    p("|" + "---|" * 17)
    for r in sorted(sel, key=key):
        x = r["result"]
        l = x["latency_ms"] or {"p50": float("nan"), "p99": float("nan")}
        at = r["issue_at"]
        if at == "farthest":
            at += f" (node {x['issue_node']}, {x['issue_rtt_to_leader_ms']:g} ms)"
        flags = ""
        if x["errors"]:
            flags += f" errors {x['errors']}"
        if x["leader_changed"]:
            flags += " LEADER CHANGED"
        if not x["all_nodes_applied_same_entries"]:
            flags += " NODES DIFFER"
        p(
            f"| {rtt_label(r)} | {len(r['voters'])} | {len(r['learners'])} | {r['size']} | {r['writers']} "
            f"| {'on' if r['batching'] else 'off'} | {r.get('max_entries_in_flight') or (2 if r['batching'] else '-')} | {at} "
            f"| {x['quorum_rtt_ms']:.1f} | {l['p50']:.2f} | {l['p99']:.2f} | {l['p50'] - x['quorum_rtt_ms']:.2f} "
            f"| {x['writes_per_s']:.0f} | {x['entries_per_write']:.3f} "
            f"| {x['voter_append_requests_with_entries_per_write']:.3f} | {x['samples']} | {r['duration_s']:.0f}{flags} |"
        )


def group_commit_table(cells, p):
    sel = [r for r in cells if r["group"] == "log_group_commit"]
    if not sel:
        return
    p("### Log writer: one fdatasync per append vs. group commit (3 voters, 200 B, batching off, at the leader)")
    p("")
    p("| RTT ms | writers | build | rep | p50 ms | p99 ms | max ms | writes/s | leader changed | samples | window s |")
    p("|---|---|---|---|---|---|---|---|---|---|---|")

    def key(r):
        return (r["delays"]["default_rtt_ms"], r["writers"], r["variant"] != "per-append", r["name"])

    for r in sorted(sel, key=key):
        x = r["result"]
        l = x["latency_ms"]
        rep = r["name"].rsplit("rep=", 1)[-1]
        flags = f" errors {x['errors']}" if x["errors"] else ""
        if not x["all_nodes_applied_same_entries"]:
            flags += " NODES DIFFER"
        p(
            f"| {r['delays']['default_rtt_ms']:g} | {r['writers']} | {r['variant']} | {rep} "
            f"| {l['p50']:.2f} | {l['p99']:.2f} | {l['max']:.1f} | {x['writes_per_s']:.0f} "
            f"| {'yes' if x['leader_changed'] else 'no'} | {x['samples']} | {r['duration_s']:.0f}{flags} |"
        )
    p("")


def learner_lag_table(cells, p):
    sel = [r for r in cells if r["learners"]]
    if not sel:
        return
    p("### Follower lag behind the leader's log (sampled every 5 ms during the window)")
    p("")
    p("| cell | follower | voter | RTT to leader ms | applied lag entries mean / max | stored lag entries mean / max | applied lag ms p50 / p99 / max |")
    p("|---|---|---|---|---|---|---|")
    for r in sorted(sel, key=lambda r: r["name"]):
        for fid, f in sorted(r["result"]["lag"].items(), key=lambda kv: int(kv[0])):
            a, s, t = f["applied_lag_entries"], f["stored_lag_entries"], f["applied_lag_ms"]
            region = r.get("regions", {}).get(fid, "")
            p(
                f"| `{r['name']}` | {fid} {region} | {'yes' if f['voter'] else 'no'} | {f['rtt_to_leader_ms']:.1f} "
                f"| {fmt_mm(a)} | {fmt_mm(s)} "
                f"| {t['p50']:.1f} / {t['p99']:.1f} / {t['max']:.1f} |" if t else "| - |"
            )
    p("")


def fmt_mm(v):
    return "-" if v is None else f"{v['mean']:.1f} / {v['max']:.0f}"


def failover_table(rows, p):
    sel = [r for r in rows.values() if r["kind"] == "failover" and "result" in r]
    if not sel:
        return
    p("### Failover: SIGKILL of the leader to the next acknowledged write (3 voters, timeouts from for_max_rtt)")
    p("")
    p("| RTT ms | heartbeat ms | election timeout ms | trials | kill → new leader seen p50 ms | kill → ack min | p50 | max ms |")
    p("|---|---|---|---|---|---|---|---|")
    for r in sorted(sel, key=lambda r: r["rtt_ms"]):
        x = r["result"]
        t = x["timeouts"]
        k = x["kill_to_ack_ms"]
        seen = sorted(tr["kill_to_new_leader_ms"] for tr in x["trials"] if tr["kill_to_new_leader_ms"] is not None)
        seen_p50 = seen[len(seen) // 2] if seen else float("nan")
        p(
            f"| {r['rtt_ms']:g} | {t['heartbeat_ms']} | {t['election_timeout_min_ms']}–{t['election_timeout_max_ms']} "
            f"| {len(x['trials'])} | {seen_p50:.0f} | {k['min']:.0f} | {k['p50']:.0f} | {k['max']:.0f} |"
        )
    p("")


def transfer_table(rows, p):
    sel = [r for r in rows.values() if r["kind"] == "transfer" and "result" in r]
    if not sel:
        return
    p("### Leader transfer (3 voters, 8 writers at the old leader; ms from the call, p50 of the trials)")
    p("")
    p("| RTT ms | in flight | trials | target is leader | old leader sees it | write issued at call at new leader | write issued when seen, at new | at old | longest gap between acks |")
    p("|---|---|---|---|---|---|---|---|---|")
    for r in sorted(sel, key=lambda r: (r["rtt_ms"], r["max_entries_in_flight"])):
        x = r["result"]
        g = lambda f: f"{x[f]['p50']:.0f}"
        p(
            f"| {r['rtt_ms']:g} | {r['max_entries_in_flight']} | {len(x['trials'])} | {g('target_leader_ms')} "
            f"| {g('old_leader_sees_new_ms')} | {g('write_at_call_at_new_ms')} | {g('write_when_seen_at_new_ms')} "
            f"| {g('write_when_seen_at_old_ms')} | {g('longest_gap_between_acks_ms')} |"
        )
    p("")


if __name__ == "__main__":
    main(sys.argv[1])
