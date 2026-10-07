import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { env } from "./env.js";

export const pool = new pg.Pool({
  connectionString: env.databaseUrl,
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

// A dropped idle connection (DB restart, failover) must not crash the process.
pool.on("error", (err) => console.error("[db] idle client error:", err.message));

/** Tiny migration runner: applies migrations/*.sql in order, once, guarded by an advisory lock so two instances can boot together. */
export async function migrate() {
  // Works from src/ (tsx dev) and from dist/src/ (compiled): use whichever folder exists.
  const here = path.dirname(fileURLToPath(import.meta.url));
  const dir = [path.resolve(here, "../migrations"), path.resolve(here, "../../migrations")].find((d) => fs.existsSync(d));
  if (!dir) throw new Error("migrations folder not found");
  const client = await pool.connect();
  try {
    await client.query("select pg_advisory_lock(727274)");
    await client.query("create table if not exists _migrations (name text primary key, applied_at timestamptz not null default now())");
    const done = new Set((await client.query("select name from _migrations")).rows.map((r) => r.name as string));
    for (const file of fs.readdirSync(dir).filter((f) => f.endsWith(".sql")).sort()) {
      if (done.has(file)) continue;
      console.log(`[db] applying ${file}`);
      await client.query("begin");
      try {
        await client.query(fs.readFileSync(path.join(dir, file), "utf8"));
        await client.query("insert into _migrations(name) values ($1)", [file]);
        await client.query("commit");
      } catch (e) {
        await client.query("rollback");
        throw e;
      }
    }
  } finally {
    await client.query("select pg_advisory_unlock(727274)").catch(() => {});
    client.release();
  }
}
