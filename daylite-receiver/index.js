import express from "express";
import crypto from "crypto";
import { PubSub } from "@google-cloud/pubsub";

console.log("RECEIVER VERSION: daylite-webhook -> pubsub 2026-02-12 (RAW parser v3 + event_type query)");

const app = express();

// IMPORTANT: with RAW, req.body is a Buffer for ALL POSTs (unless empty)
app.use(
  express.raw({
    type: "*/*",
    limit: "2mb",
  })
);

const pubsub = new PubSub();

const TOPIC = process.env.EVENTS_TOPIC || "daylite-events";
const SECRET = (process.env.WEBHOOK_SECRET || "").trim(); // can be "a,b,c" for rotation
const REQUIRE_SECRET = (process.env.REQUIRE_SECRET || "true").toLowerCase() === "true";

// Basic request logging
app.use((req, _res, next) => {
  console.log(
    "REQ",
    req.method,
    req.path,
    "ct=",
    req.get("content-type") || "-",
    "ua=",
    req.get("user-agent") || "-",
    "len=",
    req.get("content-length") || "-"
  );
  next();
});

app.get("/", (_req, res) => res.status(200).send("ok"));
app.get("/healthz", (_req, res) => res.status(200).send("ok"));

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a || ""), "utf8");
  const bb = Buffer.from(String(b || ""), "utf8");
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function isAuthorized(provided) {
  if (!REQUIRE_SECRET) return true;
  if (!SECRET) return false;

  const providedStr = String(provided || "").trim();
  if (!providedStr) return false;

  const allowed = SECRET
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  return allowed.some((s) => timingSafeEq(s, providedStr));
}

function parseBody(req) {
  const rawBuf = Buffer.isBuffer(req.body) ? req.body : Buffer.from("", "utf8");
  const ct = (req.get("content-type") || "").toLowerCase();
  const rawText = rawBuf.toString("utf8");

  // JSON
  if (ct.includes("application/json") || ct.includes("+json")) {
    if (!rawText.trim()) return { parsedBody: {}, rawText };
    try {
      return { parsedBody: JSON.parse(rawText), rawText };
    } catch {
      return { parsedBody: {}, rawText };
    }
  }

  // x-www-form-urlencoded
  if (ct.includes("application/x-www-form-urlencoded")) {
    const params = new URLSearchParams(rawText);
    const obj = {};
    for (const [k, v] of params.entries()) obj[k] = v;
    return { parsedBody: obj, rawText };
  }

  // fallback
  return { parsedBody: {}, rawText };
}

function pickProvidedSecret(req, parsedBody) {
  return (
    req.get("x-webhook-secret") ||
    req.get("x-daylite-webhook-secret") ||
    req.get("x-hook-secret") ||
    req.query?.secret ||
    parsedBody?.secret ||
    ""
  );
}

/**
 * Extract resource_url and event_type from:
 * - query string: ?event_type=project.insert (preferred)
 * - body fields (fallback)
 */
function extractHints(req, body) {
  const resourceUrl =
    body?.resource_url ||
    body?.resourceUrl ||
    body?.body?.resource_url ||
    body?.data?.resource_url ||
    null;

  // Prefer explicit event_type from URL (set per subscription target_url)
  const eventTypeFromQuery =
    req?.query?.event_type ||
    req?.query?.event ||
    req?.query?.action ||
    null;

  // Fallback to anything Daylite might include in the body (often empty)
  const eventTypeFromBody =
    body?.type ||
    body?.event_type ||
    body?.event ||
    body?.action ||
    body?.body?.action ||
    null;

  const eventType = eventTypeFromQuery || eventTypeFromBody || null;

  return { resourceUrl, eventType };
}

app.post("/webhook/daylite", async (req, res) => {
  try {
    const { parsedBody, rawText } = parseBody(req);

    const provided = pickProvidedSecret(req, parsedBody);
    if (!isAuthorized(provided)) {
      return res.status(401).send("unauthorized");
    }

    const rawBytes = Buffer.byteLength(rawText || "", "utf8");
    const MAX_PUBSUB_BYTES = Number(process.env.MAX_PUBSUB_BYTES || 9_000_000);

    let payload;
    if (rawBytes > MAX_PUBSUB_BYTES) {
      payload = {
        received_at: new Date().toISOString(),
        too_large: true,
        raw_bytes: rawBytes,
        headers: req.headers,
        body_hint: {
          keys: parsedBody && typeof parsedBody === "object" ? Object.keys(parsedBody) : [],
        },
      };
    } else {
      payload = {
        received_at: new Date().toISOString(),
        headers: req.headers,
        body: parsedBody,
        raw_body: rawText,
      };
    }

    const { resourceUrl, eventType } = extractHints(req, parsedBody);

    // Enrich payload so downstream can reliably detect action/event_type
    if (eventType) {
      payload.event_type = eventType; // top-level
      payload.body = payload.body && typeof payload.body === "object" ? payload.body : {};
      payload.body.event_type = payload.body.event_type || eventType;

      // If eventType looks like "project.insert", also provide action="insert"
      const parts = String(eventType).split(".");
      const action = parts.length ? parts[parts.length - 1] : eventType;
      payload.body.action = payload.body.action || action;
    }

    const attributes = {
      source: "daylite_webhook",
      path: req.path,
      method: req.method,
      content_type: req.get("content-type") || "",
      user_agent: req.get("user-agent") || "",
      resource_url: resourceUrl || "",
      event_type: eventType || "",
    };

    await pubsub.topic(TOPIC).publishMessage({ json: payload, attributes });

    return res.status(204).send();
  } catch (e) {
    console.error("receiver failed", e?.message || e);
    return res.status(500).send("error");
  }
});

app.listen(process.env.PORT || 8080, () => {
  console.log("listening on", process.env.PORT || 8080);
});