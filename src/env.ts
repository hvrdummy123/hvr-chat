import "dotenv/config";

function required(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var ${name}`);
  return v;
}

export const env = {
  port: Number(process.env.PORT ?? 4001),
  instanceId: process.env.INSTANCE_ID ?? `chat-${process.pid}`,
  databaseUrl: required("DATABASE_URL"),
  authUrl: required("AUTH_URL").replace(/\/$/, ""),
  // Where to fetch the public signing keys. Defaults to AUTH_URL; on one VM point it straight at hvr-auth (http://localhost:4000/api/auth/jwks).
  jwksUrl: process.env.JWKS_URL ?? `${required("AUTH_URL").replace(/\/$/, "")}/api/auth/jwks`,
  corsOrigins: (process.env.CORS_ORIGINS ?? "http://localhost:5173,http://localhost:5174")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),
  trustProxy: process.env.TRUST_PROXY === "true",
  socketBurst: Number(process.env.SOCKET_MSG_BURST ?? 20),
  socketRefillSeconds: Number(process.env.SOCKET_MSG_REFILL_SECONDS ?? 10),
};
