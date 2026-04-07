import express from "express";
import { BigQuery } from "@google-cloud/bigquery";
import { GoogleAuth } from "google-auth-library";

console.log("WORKER VERSION: daylite-generic + bq + snapshot-debug 2026-02-12");

const app = express();
app.use(express.json({ limit: "2mb" }));

// BigQuery client
const bq = new BigQuery();

// Env
const DATASET = process.env.BQ_DATASET || "daylite_db";
const EVENTS_TABLE = process.env.BQ_EVENTS_TABLE || "worker_events_raw";

// Changelog routing
// - If BQ_CHANGELOG_TABLE is set -> single table for all entities (your case: projects_changelog2)
const SINGLE_CHANGELOG_TABLE = process.env.BQ_CHANGELOG_TABLE || "";
const CHANGELOG_TEMPLATE = process.env.BQ_CHANGELOG_TABLE_TEMPLATE || "{entity_type}_changelog";

// Snapshot fetch
const ENABLE_SNAPSHOT_FETCH = (process.env.ENABLE_SNAPSHOT_FETCH || "true").toLowerCase() === "true";

// Retry behavior (Pub/Sub push subscription retries on non-2xx)
const ALLOW_PUBSUB_RETRY_ON_ERROR =
  (process.env.ALLOW_PUBSUB_RETRY_ON_ERROR || "false").toLowerCase() === "true";

// daylite-auth (Cloud Run URL)
const DAYLITE_AUTH_URL = process.env.DAYLITE_AUTH_URL || "";

// Startup env debug (AFTER consts exist!)
console.log(
  "ENV_DEBUG " +
    JSON.stringify({
      GOOGLE_CLOUD_PROJECT: process.env.GOOGLE_CLOUD_PROJECT,
      DATASET,
      EVENTS_TABLE,
      SINGLE_CHANGELOG_TABLE,
      CHANGELOG_TEMPLATE,
      ENABLE_SNAPSHOT_FETCH,
      ALLOW_PUBSUB_RETRY_ON_ERROR,
      DAYLITE_AUTH_URL_SET: !!DAYLITE_AUTH_URL,
    })
);

// ----- call daylite-auth using IAM -----
async function callDayliteAuth(path) {
  if (!DAYLITE_AUTH_URL) throw new Error("Missing DAYLITE_AUTH_URL env var");
  const auth = new GoogleAuth();
  const client = await auth.getIdTokenClient(DAYLITE_AUTH_URL);
  const r = await client.request({ url: `${DAYLITE_AUTH_URL}${path}`, method: "GET" });
  return r.data;
}

async function getAccessTokenFromAuth() {
  const data = await callDayliteAuth("/token");
  if (!data?.access_token) throw new Error("daylite-auth returned no access_token");
  return data.access_token;
}

async function forceRefreshInAuth() {
  await callDayliteAuth("/refresh");
}

async function fetchWithDayliteAuth(url) {
  let accessToken = await getAccessTokenFromAuth();

  let resp = await fetch(url, {
    headers: { Authorization: `Bearer ${accessToken}`, Accept: "application/json" },
  });

  if (resp.status === 401 || resp.status === 403) {
    await forceRefreshInAuth();
    accessToken = await getAccessTokenFromAuth();
    resp = await fetch(url, {
      headers: { Authorization: `Bearer ${accessToken}`, Accept: "application/json" },
    });
  }

  return resp;
}

function safeTableId(s) {
  // BigQuery table ids: letters, numbers, underscores only
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function normalizeUrl(u) {
  try {
    const url = new URL(u);
    url.hash = "";
    url.search = "";
    return url.toString();
  } catch {
    return u || "";
  }
}

/**
 * Parse Daylite resource URL into:
 * - entityType: first segment after /v1/
 * - entityId: next segment (if any)
 * - labelType: for /v1/labels/{label_type}
 * - snapshotUrl: normalized parent resource URL
 */
function parseDayliteResource(resourceUrl) {
  if (!resourceUrl) {
    return { resourceUrl: null, entityType: null, entityId: null, labelType: null, snapshotUrl: null };
  }

  const cleaned = normalizeUrl(resourceUrl);

  let url;
  try {
    url = new URL(cleaned);
  } catch {
    return { resourceUrl: cleaned, entityType: null, entityId: null, labelType: null, snapshotUrl: null };
  }

  const parts = url.pathname.split("/").filter(Boolean);
  const v1i = parts.indexOf("v1");
  if (v1i < 0 || v1i + 1 >= parts.length) {
    return { resourceUrl: cleaned, entityType: null, entityId: null, labelType: null, snapshotUrl: null };
  }

  const entityType = parts[v1i + 1] || null;
  const seg2 = parts[v1i + 2] || null; // often the id, or label_type for labels
  const entityId = seg2;

  // labels special: /v1/labels/{label_type}
  if (entityType === "labels") {
    const labelType = seg2;
    const snapshotUrl = labelType
      ? `${url.protocol}//${url.host}/v1/labels/${encodeURIComponent(labelType)}`
      : null;

    return {
      resourceUrl: cleaned,
      entityType,
      entityId: labelType || null,
      labelType: labelType || null,
      snapshotUrl,
    };
  }

  // If we have an id: prefer parent resource snapshot: /v1/{entityType}/{id}
  if (entityType && entityId) {
    const snapshotUrl = `${url.protocol}//${url.host}/v1/${encodeURIComponent(entityType)}/${encodeURIComponent(
      entityId
    )}`;
    return { resourceUrl: cleaned, entityType, entityId, labelType: null, snapshotUrl };
  }

  // No id -> cannot snapshot a specific entity
  return { resourceUrl: cleaned, entityType, entityId: null, labelType: null, snapshotUrl: null };
}

function extractAction(payloadObj) {
  return (
    payloadObj?.body?.action ||
    payloadObj?.body?.event ||
    payloadObj?.action ||
    payloadObj?.event ||
    "update"
  );
}

function changelogTableFor(parsed) {
  // Your setup: single table (e.g. projects_changelog2)
  if (SINGLE_CHANGELOG_TABLE) return SINGLE_CHANGELOG_TABLE;

  if (!parsed?.entityType) return null;

  if (parsed.entityType === "labels" && parsed.labelType) {
    return safeTableId(`labels_${parsed.labelType}`);
  }

  // only snapshot changelog when we have a concrete id
  if (!parsed.entityId) return null;

  const tmpl = CHANGELOG_TEMPLATE.replace("{entity_type}", safeTableId(parsed.entityType));
  return safeTableId(tmpl);
}

/**
 * Robust BigQuery insert:
 * - inserts rowObj (must match schema)
 * - uses insertId for de-dupe
 */
async function bqInsert(tableId, insertId, rowObj) {
  try {
    await bq.dataset(DATASET).table(tableId).insert([rowObj], {
      insertId,
      skipInvalidRows: false,
      ignoreUnknownValues: false,
    });
  } catch (e) {
    console.error("BQ_INSERT_FAILED " + JSON.stringify({ tableId, insertId }));
    console.error(e);
    throw e;
  }
}

app.get("/", (_req, res) => res.status(200).send("ok"));
app.get("/healthz", (_req, res) => res.status(200).send("ok"));

app.post("/pubsub/events", async (req, res) => {
  try {
    const msg = req.body?.message;
    if (!msg?.data) return res.status(400).send("bad request: missing message.data");

    const decoded = Buffer.from(msg.data, "base64").toString("utf8");

    let payloadObj;
    try {
      payloadObj = JSON.parse(decoded);
    } catch {
      payloadObj = { raw: decoded };
    }

    const resourceUrl =
      payloadObj?.body?.resource_url ||
      payloadObj?.body?.resourceUrl ||
      payloadObj?.resource_url ||
      payloadObj?.resourceUrl ||
      null;

    const parsed = parseDayliteResource(resourceUrl);
    const action = extractAction(payloadObj);

    console.log(
      "PARSED_DEBUG " +
        JSON.stringify({
          event_id: msg.messageId,
          resourceUrl,
          entityType: parsed.entityType,
          entityId: parsed.entityId,
          snapshotUrl: parsed.snapshotUrl,
          changelogTable: changelogTableFor(parsed),
          snapshotEnabled: ENABLE_SNAPSHOT_FETCH,
        })
    );

    // 1) Always write raw event (worker-owned table!)
    const eventRow = {
      received_at: payloadObj?.received_at
        ? new Date(payloadObj.received_at).toISOString()
        : new Date().toISOString(),
      event_id: msg.messageId,
      source: "daylite_webhook",
      entity_type: parsed.entityType,
      entity_id: parsed.entityId,
      action,
      resource_url: parsed.resourceUrl,
      payload: JSON.stringify(payloadObj),
    };

    await bqInsert(EVENTS_TABLE, msg.messageId, eventRow);

    // 2) Snapshot -> changelog
    if (ENABLE_SNAPSHOT_FETCH && parsed.snapshotUrl) {
      const tableId = changelogTableFor(parsed);

      console.log(
        "SNAPSHOT_START " +
          JSON.stringify({
            event_id: msg.messageId,
            snapshotUrl: parsed.snapshotUrl,
            tableId,
          })
      );

      if (!tableId) {
        throw new Error("NO_TABLE_ID for snapshot (parsed=" + JSON.stringify(parsed) + ")");
      }

      const resp = await fetchWithDayliteAuth(parsed.snapshotUrl);

      let entityJson = null;
      let errText = null;

      if (resp.ok) {
        const ct = resp.headers.get("content-type") || "";
        if (ct.includes("application/json")) {
          const json = await resp.json();
          entityJson = JSON.stringify(json);
        } else {
          const t = await resp.text();
          entityJson = JSON.stringify({ non_json: true, content_type: ct, body: t.slice(0, 2000) });
        }
      } else {
        try {
          errText = await resp.text();
        } catch {
          errText = null;
        }
      }

      const changeRow = {
        fetched_at: new Date().toISOString(),
        entity_type: parsed.entityType,
        entity_id: parsed.entityId,
        resource_url: parsed.snapshotUrl,
        http_status: resp.status,
        entity_json: entityJson,
        error_text: resp.ok ? null : errText ? errText.slice(0, 5000) : null,
      };

      console.log(
        "BQ_CHANGELOG_INSERT " +
          JSON.stringify({
            tableId,
            insertId: `${msg.messageId}-snapshot`,
            fetched_at: changeRow.fetched_at,
            entity_id: changeRow.entity_id,
            http_status: changeRow.http_status,
          })
      );

      await bqInsert(tableId, `${msg.messageId}-snapshot`, changeRow);

      console.log(
        "BQ_CHANGELOG_INSERT_OK " +
          JSON.stringify({
            tableId,
            insertId: `${msg.messageId}-snapshot`,
          })
      );

      console.log(
        "SNAPSHOT_DONE " +
          JSON.stringify({
            event_id: msg.messageId,
            http_status: resp.status,
          })
      );

      if (!resp.ok && ALLOW_PUBSUB_RETRY_ON_ERROR) {
        throw new Error(`snapshot fetch failed: ${resp.status} ${errText || ""}`);
      }
    } else {
      console.log(
        "SNAPSHOT_SKIP " +
          JSON.stringify({
            event_id: msg.messageId,
            enabled: ENABLE_SNAPSHOT_FETCH,
            snapshotUrl: parsed.snapshotUrl,
          })
      );
    }

    return res.status(204).send();
  } catch (e) {
    console.error("HANDLER_FAILED " + (e?.message || String(e)));
    console.error(e);

    if (ALLOW_PUBSUB_RETRY_ON_ERROR) {
      return res.status(500).send("retry");
    }

    return res.status(204).send();
  }
});

app.listen(process.env.PORT || 8080, () => {
  console.log("listening on", process.env.PORT || 8080);
});