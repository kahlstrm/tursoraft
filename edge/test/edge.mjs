// Starts the Worker and the follower Durable Object under Miniflare, built
// from src/ with esbuild. Used by the tests here and, through serve.mjs, by
// the Rust tests in crates/relay.

import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const entry = fileURLToPath(new URL("../src/index.ts", import.meta.url));
export const WORKER = "tursoraft-edge";

export async function bundle() {
  const out = await build({
    entryPoints: [entry],
    bundle: true,
    format: "esm",
    target: "es2022",
    external: ["cloudflare:workers"],
    write: false,
  });
  return out.outputFiles[0].text;
}

/// A running Worker. `url` is its base URL; `evict(name)` removes follower
/// `name` from memory, as Cloudflare does with an idle object, and keeps its
/// WebSockets open (hibernated).
export async function startEdge({ secret = "test-secret", port = 0 } = {}) {
  const mf = new Miniflare({
    name: WORKER,
    modules: true,
    script: await bundle(),
    compatibilityDate: "2026-07-30",
    durableObjects: { FOLLOWER: { className: "Follower", useSQLite: true } },
    bindings: { RELAY_SECRET: secret, FOLLOWER_NAME: "default" },
    host: "127.0.0.1",
    // No request.cf: Miniflare would fetch it and cache it in .wrangler/.
    cf: false,
    port,
  });
  const url = await mf.ready;
  return {
    mf,
    url: url.href.replace(/\/$/, ""),
    secret,
    evict: (name) =>
      mf.unsafeEvictDurableObject(WORKER, "Follower", { name, webSockets: "hibernate" }),
    dispose: () => mf.dispose(),
  };
}
