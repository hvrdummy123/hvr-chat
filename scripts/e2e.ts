/**
 * End-to-end smoke test. Needs hvr-auth and hvr-chat running (and Postgres).
 *   npm run test:e2e
 * Optional: CHAT_URL_B=http://localhost:4002 to put the two users on DIFFERENT chat instances
 * (proves cross-instance delivery through Postgres LISTEN/NOTIFY).
 */
import { randomUUID } from "node:crypto";
import { decodeJwt } from "jose";
import { io, type Socket } from "socket.io-client";

const AUTH = process.env.AUTH_URL ?? "http://localhost:4000";
const CHAT_A = process.env.CHAT_URL ?? "http://localhost:4001";
const CHAT_B = process.env.CHAT_URL_B ?? CHAT_A;
const ORIGIN = "http://localhost:5173";
const run = Date.now().toString(36);

let failed = 0;
function check(name: string, ok: boolean, extra = "") {
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${extra ? "  — " + extra : ""}`);
  if (!ok) failed++;
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function makeUser(label: string) {
  const email = `${label}-${run}@hvr.test`;
  const password = `Pw-${run}-Strong!42`;
  const res = await fetch(`${AUTH}/api/auth/sign-up/email`, {
    method: "POST",
    headers: { "content-type": "application/json", origin: ORIGIN },
    body: JSON.stringify({ email, password, name: `${label} ${run}` }),
  });
  if (!res.ok) throw new Error(`sign-up failed (${res.status}): ${await res.text()}`);
  const session = res.headers.get("set-auth-token");
  if (!session) throw new Error("no set-auth-token header (is the bearer plugin enabled?)");
  const t = await fetch(`${AUTH}/api/auth/token`, { headers: { authorization: `Bearer ${session}`, origin: ORIGIN } });
  if (!t.ok) throw new Error(`token failed (${t.status}): ${await t.text()}`);
  const { token } = (await t.json()) as { token: string };
  return { token, id: decodeJwt(token).sub as string };
}

function connect(url: string, token: string): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const s = io(url, { auth: { token }, transports: ["websocket"], reconnection: false });
    s.once("connect", () => resolve(s));
    s.once("connect_error", (e) => reject(e));
  });
}

const api = (base: string, token: string, path: string, init: RequestInit = {}) =>
  fetch(`${base}${path}`, {
    ...init,
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json", ...(init.headers ?? {}) },
  });

function waitFor<T>(s: Socket, event: string, ms = 3000): Promise<T | null> {
  return new Promise((resolve) => {
    const t = setTimeout(() => resolve(null), ms);
    s.once(event, (d: T) => {
      clearTimeout(t);
      resolve(d);
    });
  });
}

const send = (s: Socket, conversationId: string, body: string, clientId = randomUUID()) =>
  s.timeout(5000).emitWithAck("message:send", { conversationId, clientId, body }) as Promise<any>;

async function main() {
  const alice = await makeUser("alice");
  const bob = await makeUser("bob");
  const carol = await makeUser("carol");

  const sa = await connect(CHAT_A, alice.token);
  let sb = await connect(CHAT_B, bob.token);
  const sc = await connect(CHAT_A, carol.token);

  const convRes = await api(CHAT_A, alice.token, "/conversations", { method: "POST", body: JSON.stringify({ userId: bob.id }) });
  const { id: convId } = (await convRes.json()) as { id: string };
  check("open DM (find-or-create)", !!convId);
  const again = await api(CHAT_A, bob.token, "/conversations", { method: "POST", body: JSON.stringify({ userId: alice.id }) });
  check("DM is the same conversation from the other side", ((await again.json()) as { id: string }).id === convId);

  // 1. realtime delivery
  const gotP = waitFor<any>(sb, "message:new");
  const ack1 = await send(sa, convId, "hello bob");
  const got = await gotP;
  check("1. message is acked after being stored", ack1.ok && ack1.message.seq === 1);
  check("1. recipient receives it in real time", got?.body === "hello bob" && got?.seq === 1);

  // 2. idempotent retry (client never saw the ack and re-sends the same clientId)
  const cid = randomUUID();
  const seen: any[] = [];
  sb.on("message:new", (m) => m.body === "retry me" && seen.push(m));
  const first = await send(sa, convId, "retry me", cid);
  const second = await send(sa, convId, "retry me", cid);
  await sleep(700);
  check("2. re-sending the same clientId does not create a duplicate", second.ok && second.duplicate === true && second.message.id === first.message.id);
  check("2. recipient is notified exactly once", seen.length === 1, `deliveries=${seen.length}`);
  sb.off("message:new");

  // 3. recipient offline -> catch up by seq
  sb.disconnect();
  await send(sa, convId, "while you were away 1");
  await send(sa, convId, "while you were away 2");
  await send(sa, convId, "while you were away 3");
  const hist = await api(CHAT_B, bob.token, `/conversations/${convId}/messages?afterSeq=2`);
  const missed = (await hist.json()) as any[];
  check("3. offline user fetches missed messages by seq, in order", missed.length === 3 && missed.map((m) => m.seq).join() === "3,4,5");
  sb = await connect(CHAT_B, bob.token);
  check("3. offline user can reconnect", sb.connected);
  const convs = (await (await api(CHAT_B, bob.token, "/conversations")).json()) as any[];
  check("3. unread count reflects missed messages", convs[0]?.unread === 5, `unread=${convs[0]?.unread}`);

  // 4. bad / missing token
  const bad = await connect(CHAT_A, "garbage").then(() => false, () => true);
  check("4. invalid token is rejected", bad);
  const noAuth = await fetch(`${CHAT_A}/conversations`);
  check("4. REST without token is 401", noAuth.status === 401);

  // 5. non-member
  const intruder = await send(sc, convId, "let me in");
  check("5. non-member cannot send", intruder.ok === false && intruder.error === "not_a_member");
  const peek = await api(CHAT_A, carol.token, `/conversations/${convId}/messages`);
  check("5. non-member cannot read history", peek.status === 403);

  // 6. validation
  const empty = await send(sa, convId, "   ");
  check("6. empty message rejected", empty.ok === false);
  const huge = await send(sa, convId, "x".repeat(1001));
  check("6. over-long message rejected", huge.ok === false);

  // 7. non-admin cannot use admin routes
  const adm = await api(CHAT_A, alice.token, "/admin/stats");
  check("7. admin routes are forbidden for normal users", adm.status === 403);

  // 8. flood protection (last: it eats the bucket)
  const results = await Promise.all(Array.from({ length: 40 }, (_, i) => send(sa, convId, `flood ${i}`)));
  const limited = results.filter((r) => r.error === "rate_limited").length;
  check("8. socket flood is rate limited", limited > 0, `${limited}/40 limited`);

  [sa, sb, sc].forEach((s) => s.disconnect());
  console.log(failed ? `\n${failed} check(s) FAILED` : "\nAll checks passed");
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error("e2e crashed:", e);
  process.exit(1);
});
