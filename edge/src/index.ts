// The Worker: renders HTML pages from a follower Durable Object, and hands
// a relay's WebSocket to the follower.
//
//   GET  /                     products, from the follower
//   GET  /products/<id>        one product
//        ?at=<log index>       wait until the follower has applied it first
//        ?wait_ms=<ms>         the most to wait (default 5000, at most 30000)
//        ?follower=<name>      which follower (default FOLLOWER_NAME)
//   GET  /relay                a relay's WebSocket                  (secret)
//   GET  /admin/status         the follower's position              (secret)
//   GET  /admin/dump           every copied table, exact values     (secret)
//   POST /admin/close-relays   drop the relays' WebSockets          (secret)
//
// The secret is `Authorization: Bearer <RELAY_SECRET>`. Pages are public
// and read only; nothing writes through this Worker.

import { Follower, type Env } from "./follower";
import { html, layout, page } from "./pages";

export { Follower };

const DEFAULT_WAIT_MS = 5_000;
const MAX_WAIT_MS = 30_000;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const follower = env.FOLLOWER.get(
      env.FOLLOWER.idFromName(url.searchParams.get("follower") ?? env.FOLLOWER_NAME ?? "default"),
    );
    if (url.pathname === "/relay" || url.pathname.startsWith("/admin/")) {
      const refused = await checkSecret(request, env);
      if (refused) return refused;
      return admin(request, url, follower);
    }
    if (request.method !== "GET") return new Response("method not allowed", { status: 405 });
    const found = page(url.pathname);
    if (!found) return htmlResponse(404, layout("Not found", "<h1>Not found</h1>"));
    const wait = readWait(url);
    if (wait instanceof Response) return wait;
    let result;
    try {
      result = await follower.read(found.query.sql, found.query.params, wait.at, wait.ms);
    } catch (e) {
      // Such as "no such table" before the first copy.
      const message = e instanceof Error ? e.message : String(e);
      return htmlResponse(503, layout("Unavailable", `<h1>Unavailable</h1><p>${html(message)}</p>`), null);
    }
    const headers = { "x-follower-through": String(result.through) };
    if (result.rows === null) {
      return htmlResponse(
        504,
        layout("Timed out", `<h1>Timed out</h1><p>The follower has not applied log index ${wait.at}.</p>`),
        result.through,
      );
    }
    const body = found.render(result.rows);
    if (body === null) return htmlResponse(404, layout("Not found", "<h1>Not found</h1>"), result.through);
    return new Response(body, {
      headers: { ...headers, "content-type": "text/html; charset=utf-8" },
    });
  },
} satisfies ExportedHandler<Env>;

async function admin(
  request: Request,
  url: URL,
  follower: DurableObjectStub<Follower>,
): Promise<Response> {
  switch (url.pathname) {
    case "/relay":
      return follower.fetch(request);
    case "/admin/status":
      return Response.json(await follower.status());
    case "/admin/dump": {
      const wait = readWait(url);
      if (wait instanceof Response) return wait;
      const dump = await follower.dump(wait.at, wait.ms);
      return dump === null ? new Response("timed out", { status: 504 }) : Response.json(dump);
    }
    case "/admin/close-relays":
      if (request.method !== "POST") return new Response("method not allowed", { status: 405 });
      return Response.json({ closed: await follower.closeRelays() });
    default:
      return new Response("not found", { status: 404 });
  }
}

async function checkSecret(request: Request, env: Env): Promise<Response | null> {
  if (!env.RELAY_SECRET) return new Response("RELAY_SECRET is not set", { status: 503 });
  const given = new TextEncoder().encode(request.headers.get("authorization") ?? "");
  const expected = new TextEncoder().encode(`Bearer ${env.RELAY_SECRET}`);
  const equal =
    given.byteLength === expected.byteLength &&
    crypto.subtle.timingSafeEqual(given, expected);
  return equal ? null : new Response("unauthorized", { status: 401 });
}

function readWait(url: URL): { at: number | null; ms: number } | Response {
  const at = url.searchParams.get("at");
  const ms = url.searchParams.get("wait_ms");
  const atNumber = at === null ? null : Number(at);
  const msNumber = ms === null ? DEFAULT_WAIT_MS : Number(ms);
  if (atNumber !== null && !(Number.isSafeInteger(atNumber) && atNumber >= 0)) {
    return new Response("at must be a log index", { status: 400 });
  }
  if (!(Number.isFinite(msNumber) && msNumber >= 0)) {
    return new Response("wait_ms must be a number of milliseconds", { status: 400 });
  }
  return { at: atNumber, ms: Math.min(msNumber, MAX_WAIT_MS) };
}

function htmlResponse(status: number, body: string, through: number | null = null): Response {
  const headers: Record<string, string> = { "content-type": "text/html; charset=utf-8" };
  if (through !== null) headers["x-follower-through"] = String(through);
  return new Response(body, { status, headers });
}
