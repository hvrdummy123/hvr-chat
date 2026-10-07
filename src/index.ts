import cors from "cors";
import express, { type NextFunction, type Request, type Response } from "express";
import helmet from "helmet";
import http from "node:http";
import { ZodError } from "zod";
import { migrate, pool } from "./db.js";
import { env } from "./env.js";
import { HttpError } from "./errors.js";
import { attachRealtime } from "./realtime.js";
import { buildRouter } from "./routes.js";

async function main() {
  await migrate();

  const app = express();
  app.set("trust proxy", env.trustProxy ? 1 : false);
  app.use(helmet());
  app.use(cors({ origin: env.corsOrigins, credentials: true }));
  app.use(express.json({ limit: "16kb" }));

  app.get("/health", async (_req, res) => {
    try {
      await pool.query("select 1");
      res.json({ ok: true, instance: env.instanceId });
    } catch {
      res.status(503).json({ ok: false, instance: env.instanceId });
    }
  });

  app.use(buildRouter());

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof HttpError) return res.status(err.status).json({ error: err.code });
    if (err instanceof ZodError) return res.status(400).json({ error: "invalid_request" });
    console.error(`[${env.instanceId}]`, err);
    res.status(500).json({ error: "server_error" });
  });

  const server = http.createServer(app);
  const io = attachRealtime(server);
  server.listen(env.port, () => console.log(`[${env.instanceId}] hvr-chat listening on :${env.port}`));

  // Graceful shutdown (deploys, SIGTERM): stop taking connections, let clients reconnect elsewhere.
  const shutdown = (signal: string) => {
    console.log(`[${env.instanceId}] ${signal} received, shutting down`);
    io.close(() => pool.end().finally(() => process.exit(0)));
    setTimeout(() => process.exit(1), 10_000).unref();
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

// A bug in one handler should be logged, not take the whole server down silently.
process.on("unhandledRejection", (e) => console.error("[unhandledRejection]", e));

main().catch((e) => {
  console.error("Fatal startup error:", e);
  process.exit(1);
});
