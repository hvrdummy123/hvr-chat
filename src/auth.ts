import { createRemoteJWKSet, jwtVerify } from "jose";
import { pool } from "./db.js";
import { env } from "./env.js";

export type AuthUser = { id: string; name: string; email: string; role: string };

// Keys are cached for 24h and only re-fetched when an unknown key id shows up (key rotation).
// So if hvr-auth goes down, users who already have a token keep working.
const jwks = createRemoteJWKSet(new URL(env.jwksUrl), {
  cooldownDuration: 30_000,
  cacheMaxAge: 24 * 60 * 60 * 1000,
});

const lastUpsert = new Map<string, number>();

export async function authenticate(token: unknown): Promise<AuthUser> {
  if (typeof token !== "string" || !token) throw new Error("missing_token");
  const { payload } = await jwtVerify(token, jwks, { issuer: env.authUrl });
  if (!payload.sub) throw new Error("no_subject");
  const user: AuthUser = {
    id: payload.sub,
    name: String(payload.name ?? "User"),
    email: String(payload.email ?? ""),
    role: String(payload.role ?? "user"),
  };

  // Mirror the user into chat_users (at most once every 5 minutes per user per instance).
  const now = Date.now();
  if (now - (lastUpsert.get(user.id) ?? 0) > 5 * 60_000) {
    await pool.query(
      `insert into chat_users (id, name, email, role) values ($1,$2,$3,$4)
       on conflict (id) do update set name = excluded.name, email = excluded.email, role = excluded.role, last_seen_at = now()`,
      [user.id, user.name, user.email, user.role],
    );
    lastUpsert.set(user.id, now);
  }
  return user;
}
