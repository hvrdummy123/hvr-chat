import type { Server as HttpServer } from "node:http";
import pg from "pg";
import { Server } from "socket.io";
import { z } from "zod";
import { authenticate, type AuthUser } from "./auth.js";
import { env } from "./env.js";
import { HttpError } from "./errors.js";
import { markRead, NOTIFY_CHANNEL, sendMessage, type Msg } from "./messages.js";

const sendSchema = z.object({
  conversationId: z.string().uuid(),
  clientId: z.string().uuid(),
  body: z.string().trim().min(1).max(1000),
});
const readSchema = z.object({ conversationId: z.string().uuid(), seq: z.number().int().min(0) });

/** Per-connection token bucket: `burst` messages, refilled over `refillSeconds`. */
class Bucket {
  private tokens = env.socketBurst;
  private last = Date.now();
  take(): boolean {
    const now = Date.now();
    this.tokens = Math.min(env.socketBurst, this.tokens + ((now - this.last) / 1000) * (env.socketBurst / env.socketRefillSeconds));
    this.last = now;
    if (this.tokens < 1) return false;
    this.tokens -= 1;
    return true;
  }
}

export function attachRealtime(httpServer: HttpServer) {
  const io = new Server(httpServer, {
    cors: { origin: env.corsOrigins, credentials: true },
    transports: ["websocket"], // websocket only => no sticky sessions needed behind a load balancer
    pingInterval: 20_000,
    pingTimeout: 20_000,
    maxHttpBufferSize: 16 * 1024,
  });

  io.use(async (socket, next) => {
    try {
      socket.data.user = await authenticate(socket.handshake.auth?.token);
      next();
    } catch {
      next(new Error("unauthorized"));
    }
  });

  io.on("connection", (socket) => {
    const user = socket.data.user as AuthUser;
    socket.join(`user:${user.id}`); // every tab/device of a user is in the same room
    const bucket = new Bucket();

    socket.on("message:send", async (raw: unknown, ack?: (r: unknown) => void) => {
      const reply = typeof ack === "function" ? ack : () => {};
      const parsed = sendSchema.safeParse(raw);
      if (!parsed.success) return reply({ ok: false, error: "invalid_message" });
      if (!bucket.take()) return reply({ ok: false, error: "rate_limited" });
      try {
        const { message, duplicate } = await sendMessage(user.id, parsed.data);
        reply({ ok: true, message, duplicate });
      } catch (e) {
        if (e instanceof HttpError) return reply({ ok: false, error: e.code });
        console.error(`[${env.instanceId}] send failed:`, (e as Error).message);
        reply({ ok: false, error: "server_error" }); // client keeps the message as "failed" and can retry with the same clientId
      }
    });

    socket.on("message:read", async (raw: unknown) => {
      const parsed = readSchema.safeParse(raw);
      if (!parsed.success) return;
      await markRead(user.id, parsed.data.conversationId, parsed.data.seq).catch(() => {});
    });
  });

  startListener(io);
  return io;
}

/**
 * Cross-instance fan-out WITHOUT Redis: every instance LISTENs on a Postgres channel.
 * When any instance commits a message, Postgres notifies all instances; each one pushes it
 * to the sockets it holds. If this connection drops we reconnect with backoff and tell every
 * local client to resync (they fetch anything they missed by seq).
 */
function startListener(io: Server) {
  let backoff = 500;
  let first = true;
  let timer: NodeJS.Timeout | null = null;
  let current: pg.Client | null = null;

  const schedule = () => {
    if (timer) return;
    current?.removeAllListeners();
    current?.end().catch(() => {});
    current = null;
    timer = setTimeout(() => {
      timer = null;
      connect();
    }, backoff);
    backoff = Math.min(backoff * 2, 10_000);
  };

  const connect = async () => {
    const client = new pg.Client({ connectionString: env.databaseUrl, keepAlive: true });
    current = client;
    client.on("error", (e) => {
      console.error(`[${env.instanceId}] listener error:`, e.message);
      schedule();
    });
    client.on("end", schedule);
    client.on("notification", (n) => {
      if (n.channel !== NOTIFY_CHANNEL || !n.payload) return;
      try {
        const { message, recipients } = JSON.parse(n.payload) as { message: Msg; recipients: string[] };
        io.to(recipients.map((id) => `user:${id}`)).emit("message:new", message);
      } catch (e) {
        console.error(`[${env.instanceId}] bad notification:`, (e as Error).message);
      }
    });
    try {
      await client.connect();
      await client.query(`listen ${NOTIFY_CHANNEL}`);
      backoff = 500;
      console.log(`[${env.instanceId}] listening for messages`);
      if (!first) io.emit("resync"); // we may have missed notifications while disconnected
      first = false;
    } catch (e) {
      console.error(`[${env.instanceId}] listener connect failed:`, (e as Error).message);
      schedule();
    }
  };

  connect();
}
