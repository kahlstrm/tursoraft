// For the Rust tests and deploy/local.sh: starts the Worker under
// Miniflare, prints {"url": ...} as one line, then reads commands from
// stdin, one per line, and answers each with one line:
//
//   evict <follower name>   ->  evicted
//
// Stops Miniflare and exits when stdin closes, or on SIGTERM or SIGINT.
// With --until-signal it reads no stdin and runs until a signal.
// Stopping Miniflare stops its workerd process; a SIGKILL of this process
// would leave workerd running, so callers should close stdin or send
// SIGTERM first.

import { createInterface } from "node:readline";
import { startEdge } from "./edge.mjs";

const edge = await startEdge({ secret: process.env.RELAY_SECRET ?? "test-secret" });
let stopping = null;
const stop = () => {
  stopping ??= edge.dispose().finally(() => process.exit(0));
  return stopping;
};
process.on("SIGTERM", stop);
process.on("SIGINT", stop);
console.log(JSON.stringify({ url: edge.url }));

if (!process.argv.includes("--until-signal")) {
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
  await stop();
}
