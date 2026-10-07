import { pool } from "./db.js";
import { HttpError } from "./errors.js";

export const NOTIFY_CHANNEL = "hvr_chat";

export type Msg = {
  id: string;
  clientId: string;
  conversationId: string;
  seq: number;
  senderId: string;
  body: string;
  createdAt: string;
  deleted: boolean;
};

type Row = {
  id: string;
  client_id: string;
  conversation_id: string;
  seq: string;
  sender_id: string;
  body: string;
  created_at: Date;
  deleted_at: Date | null;
};

export function toMsg(r: Row): Msg {
  return {
    id: r.id,
    clientId: r.client_id,
    conversationId: r.conversation_id,
    seq: Number(r.seq),
    senderId: r.sender_id,
    body: r.deleted_at ? "" : r.body,
    createdAt: r.created_at.toISOString(),
    deleted: !!r.deleted_at,
  };
}

const COLS = "id, client_id, conversation_id, seq, sender_id, body, created_at, deleted_at";

/**
 * The heart of the system. In ONE transaction:
 *  1. check membership
 *  2. return the existing row if this clientId was already stored (idempotent retry)
 *  3. take the next seq for the conversation (row lock => no gaps, no duplicates)
 *  4. insert the message
 *  5. pg_notify (only delivered if the transaction commits)
 * The sender is acked only after COMMIT, so "acked" always means "durably stored".
 */
export async function sendMessage(
  userId: string,
  input: { conversationId: string; clientId: string; body: string },
): Promise<{ message: Msg; duplicate: boolean }> {
  const c = await pool.connect();
  try {
    await c.query("begin");

    const member = await c.query(
      "select 1 from conversation_members where conversation_id = $1 and user_id = $2",
      [input.conversationId, userId],
    );
    if (!member.rowCount) throw new HttpError(403, "not_a_member");

    const dup = await c.query(`select ${COLS} from messages where sender_id = $1 and client_id = $2`, [userId, input.clientId]);
    if (dup.rowCount) {
      await c.query("rollback");
      return { message: toMsg(dup.rows[0]), duplicate: true };
    }

    const seqRes = await c.query(
      "update conversations set last_seq = last_seq + 1, last_message_at = now() where id = $1 returning last_seq",
      [input.conversationId],
    );
    const ins = await c.query(
      `insert into messages (conversation_id, seq, sender_id, client_id, body)
       values ($1, $2, $3, $4, $5) returning ${COLS}`,
      [input.conversationId, seqRes.rows[0].last_seq, userId, input.clientId, input.body],
    );
    const message = toMsg(ins.rows[0]);

    const members = await c.query("select user_id from conversation_members where conversation_id = $1", [input.conversationId]);
    const recipients = members.rows.map((r) => r.user_id as string);
    await c.query("select pg_notify($1, $2)", [NOTIFY_CHANNEL, JSON.stringify({ message, recipients })]);

    await c.query("commit");
    return { message, duplicate: false };
  } catch (e) {
    await c.query("rollback").catch(() => {});
    // Two retries racing with the same clientId: the loser hits the unique index -> return the winner's row.
    if ((e as { code?: string }).code === "23505") {
      const r = await pool.query(`select ${COLS} from messages where sender_id = $1 and client_id = $2`, [userId, input.clientId]);
      if (r.rowCount) return { message: toMsg(r.rows[0]), duplicate: true };
    }
    throw e;
  } finally {
    c.release();
  }
}

async function assertMember(userId: string, conversationId: string) {
  const r = await pool.query("select 1 from conversation_members where conversation_id = $1 and user_id = $2", [conversationId, userId]);
  if (!r.rowCount) throw new HttpError(403, "not_a_member");
}

/** afterSeq => everything newer than N (catch-up after reconnect). Otherwise the latest page, optionally older than beforeSeq. */
export async function listMessages(
  userId: string,
  conversationId: string,
  opts: { afterSeq?: number; beforeSeq?: number; limit: number },
): Promise<Msg[]> {
  await assertMember(userId, conversationId);
  if (opts.afterSeq !== undefined) {
    const r = await pool.query(
      `select ${COLS} from messages where conversation_id = $1 and seq > $2 order by seq asc limit $3`,
      [conversationId, opts.afterSeq, opts.limit],
    );
    return r.rows.map(toMsg);
  }
  const r = await pool.query(
    `select ${COLS} from messages where conversation_id = $1 and ($2::bigint is null or seq < $2) order by seq desc limit $3`,
    [conversationId, opts.beforeSeq ?? null, opts.limit],
  );
  return r.rows.map(toMsg).reverse();
}

export async function listConversations(userId: string) {
  const r = await pool.query(
    `select c.id, c.last_seq, c.last_message_at,
            o.user_id as other_id, u.name as other_name,
            lm.body as last_body, lm.sender_id as last_sender, lm.deleted_at as last_deleted,
            (select count(*) from messages x
               where x.conversation_id = c.id and x.seq > m.last_read_seq and x.sender_id <> $1 and x.deleted_at is null) as unread
       from conversation_members m
       join conversations c on c.id = m.conversation_id
       join conversation_members o on o.conversation_id = c.id and o.user_id <> m.user_id
       join chat_users u on u.id = o.user_id
       left join lateral (
         select body, sender_id, deleted_at from messages where conversation_id = c.id order by seq desc limit 1
       ) lm on true
      where m.user_id = $1
      order by c.last_message_at desc nulls last, c.created_at desc
      limit 200`,
    [userId],
  );
  return r.rows.map((x) => ({
    id: x.id as string,
    lastSeq: Number(x.last_seq),
    lastMessageAt: x.last_message_at ? (x.last_message_at as Date).toISOString() : null,
    other: { id: x.other_id as string, name: x.other_name as string },
    lastMessage: x.last_body ? { body: x.last_deleted ? "" : (x.last_body as string), senderId: x.last_sender as string, deleted: !!x.last_deleted } : null,
    unread: Number(x.unread),
  }));
}

/** Find or create the 1:1 conversation between two users. */
export async function openDm(userId: string, otherId: string): Promise<string> {
  if (userId === otherId) throw new HttpError(400, "cannot_dm_self");
  const other = await pool.query("select 1 from chat_users where id = $1", [otherId]);
  if (!other.rowCount) throw new HttpError(404, "user_not_found");

  const key = [userId, otherId].sort().join(":");
  const c = await pool.connect();
  try {
    await c.query("begin");
    const conv = await c.query(
      "insert into conversations (dm_key) values ($1) on conflict (dm_key) do update set dm_key = excluded.dm_key returning id",
      [key],
    );
    const id = conv.rows[0].id as string;
    await c.query(
      "insert into conversation_members (conversation_id, user_id) values ($1,$2),($1,$3) on conflict do nothing",
      [id, userId, otherId],
    );
    await c.query("commit");
    return id;
  } catch (e) {
    await c.query("rollback").catch(() => {});
    throw e;
  } finally {
    c.release();
  }
}

export async function markRead(userId: string, conversationId: string, seq: number) {
  await pool.query(
    "update conversation_members set last_read_seq = greatest(last_read_seq, $3) where conversation_id = $1 and user_id = $2",
    [conversationId, userId, seq],
  );
}

export async function searchUsers(userId: string, q: string) {
  const term = q.trim().toLowerCase();
  if (term.length < 2) return [];
  // Match name prefix, or an exact email (so emails can't be harvested by browsing).
  const r = await pool.query(
    `select id, name from chat_users
      where id <> $1 and (lower(name) like $2 or lower(email) = $3)
      order by name limit 10`,
    [userId, term.replace(/[%_]/g, "") + "%", term],
  );
  return r.rows as { id: string; name: string }[];
}
