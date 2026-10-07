import { Router, type NextFunction, type Request, type Response } from "express";
import rateLimit from "express-rate-limit";
import { z } from "zod";
import { authenticate, type AuthUser } from "./auth.js";
import { pool } from "./db.js";
import { HttpError } from "./errors.js";
import { listConversations, listMessages, markRead, openDm, searchUsers } from "./messages.js";

declare module "express-serve-static-core" {
  interface Request {
    user?: AuthUser;
  }
}

const wrap =
  (fn: (req: Request, res: Response) => Promise<unknown>) =>
  (req: Request, res: Response, next: NextFunction) =>
    fn(req, res).catch(next);

async function requireUser(req: Request, _res: Response, next: NextFunction) {
  try {
    const header = req.headers.authorization ?? "";
    req.user = await authenticate(header.startsWith("Bearer ") ? header.slice(7) : "");
    next();
  } catch {
    next(new HttpError(401, "unauthorized"));
  }
}

function requireAdmin(req: Request, _res: Response, next: NextFunction) {
  if (req.user?.role !== "admin") return next(new HttpError(403, "admin_only"));
  next();
}

export function buildRouter() {
  const r = Router();

  // Cheap IP limiter before any auth work, then a per-user limiter after it.
  r.use(rateLimit({ windowMs: 60_000, limit: 300, standardHeaders: "draft-7", legacyHeaders: false }));
  r.use(requireUser);
  r.use(
    rateLimit({
      windowMs: 60_000,
      limit: 120,
      standardHeaders: "draft-7",
      legacyHeaders: false,
      keyGenerator: (req) => req.user!.id,
      validate: false,
    }),
  );

  r.get("/users", wrap(async (req, res) => res.json(await searchUsers(req.user!.id, String(req.query.q ?? "")))));

  r.get("/conversations", wrap(async (req, res) => res.json(await listConversations(req.user!.id))));

  r.post(
    "/conversations",
    wrap(async (req, res) => {
      const { userId } = z.object({ userId: z.string().min(1) }).parse(req.body);
      res.json({ id: await openDm(req.user!.id, userId) });
    }),
  );

  r.get(
    "/conversations/:id/messages",
    wrap(async (req, res) => {
      const id = z.string().uuid().parse(req.params.id);
      const q = z
        .object({
          afterSeq: z.coerce.number().int().min(0).optional(),
          beforeSeq: z.coerce.number().int().min(1).optional(),
          limit: z.coerce.number().int().min(1).max(100).default(50),
        })
        .parse(req.query);
      res.json(await listMessages(req.user!.id, id, q));
    }),
  );

  r.post(
    "/conversations/:id/read",
    wrap(async (req, res) => {
      const id = z.string().uuid().parse(req.params.id);
      const { seq } = z.object({ seq: z.number().int().min(0) }).parse(req.body);
      await markRead(req.user!.id, id, seq);
      res.json({ ok: true });
    }),
  );

  // ---- admin (role comes from the JWT issued by hvr-auth) ----
  r.get(
    "/admin/stats",
    requireAdmin,
    wrap(async (_req, res) => {
      const q = await pool.query(`
        select (select count(*) from chat_users)                                              as users,
               (select count(*) from conversations)                                           as conversations,
               (select count(*) from messages)                                                as messages,
               (select count(*) from messages where created_at > now() - interval '24 hours') as messages_24h`);
      const row = q.rows[0];
      res.json({
        users: Number(row.users),
        conversations: Number(row.conversations),
        messages: Number(row.messages),
        messages24h: Number(row.messages_24h),
      });
    }),
  );

  r.get(
    "/admin/messages",
    requireAdmin,
    wrap(async (req, res) => {
      const limit = Math.min(Number(req.query.limit ?? 50) || 50, 200);
      const q = await pool.query(
        `select m.id, m.seq, m.body, m.created_at, m.deleted_at, s.name as sender_name,
                (select string_agg(u.name, ', ') from conversation_members cm join chat_users u on u.id = cm.user_id where cm.conversation_id = m.conversation_id) as participants
           from messages m join chat_users s on s.id = m.sender_id
          order by m.created_at desc limit $1`,
        [limit],
      );
      res.json(
        q.rows.map((x) => ({
          id: x.id as string,
          seq: Number(x.seq),
          body: x.body as string,
          createdAt: (x.created_at as Date).toISOString(),
          deleted: !!x.deleted_at,
          sender: x.sender_name as string,
          participants: x.participants as string,
        })),
      );
    }),
  );

  r.delete(
    "/admin/messages/:id",
    requireAdmin,
    wrap(async (req, res) => {
      const id = z.string().uuid().parse(req.params.id);
      await pool.query("update messages set deleted_at = now() where id = $1 and deleted_at is null", [id]);
      res.json({ ok: true });
    }),
  );

  return r;
}
