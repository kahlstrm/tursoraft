// For the Rust tests: starts the Worker under Miniflare, prints
// {"url": ...} as one line, then reads commands from stdin, one per line,
// and answers each with one line:
//
//   evict <follower name>   ->  evicted
//
// Exits when stdin closes.

import { createInterface } from "node:readline";
import { startEdge } from "./edge.mjs";

const edge = await startEdge({ secret: process.env.RELAY_SECRET ?? "test-secret" });
console.log(JSON.stringify({ url: edge.url }));
for await (const line of createInterface({ input: process.stdin })) {
  const [command, arg] = line.trim().split(/\s+/);
  try {
    if (command === "evict") {
      await edge.evict(arg);
      console.log("evicted");
    } else {
      console.log(`error unknown command ${command}`);
    }
  } catch (e) {
    console.log(`error ${e instanceof Error ? e.message : e}`);
  }
}
await edge.dispose();
