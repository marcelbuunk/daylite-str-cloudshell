import express from "express";
import { BigQuery } from "@google-cloud/bigquery";

const app = express();
app.use(express.json({ limit: "2mb" }));

const bq = new BigQuery();
const DATASET = process.env.BQ_DATASET || "daylite_db";
const EVENTS_TABLE = process.env.BQ_EVENTS_TABLE || "events_raw";
const PROJECTS_TABLE = process.env.BQ_PROJECTS_TABLE || "projects_changelog";

// ✅ Personal token refresh endpoint (niet OAuth client token endpoint)
const PERSONAL_REFRESH_URL = "https://api.marketcircle.net/v1/personal_token/refresh_token";

async function refreshAccessToken() {
  const refreshToken = process.env.DAYLITE_REFRESH_TOKEN;
  if (!refreshToken) throw new Error("Missing DAYLITE_REFRESH_TOKEN");

  // Personal Token refresh is een GET naar /v1/personal_token/refresh_token
  // met Authorization: Bearer <refresh_token>
  const resp = await fetch(PERSONAL_REFRESH_URL, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${refreshToken}`,
      Accept: "application/json",
    },
  });

  if (!resp.ok) {
    const t = await resp.text();
    throw new Error(`personal token refresh failed: ${resp.status} ${t}`);
  }

  const json = await resp.json();
  if (!json?.access_token) throw new Error("personal token refresh returned no access_token");
  return json.access_token;
}

async function fetchWithAuth(url) {
  let accessToken = process.env.DAYLITE_ACCESS_TOKEN;
  if (!accessToken) throw new Error("Missing DAYLITE_ACCESS_TOKEN");

  let resp = await fetch(url, {
    headers: { Authorization: `Bearer ${accessToken}`, Accept: "application/json" },
  });

  // token verlopen → refresh via personal token endpoint → retry
  if (resp.status === 401) {
    accessToken = await refreshAccessToken();
    resp = await fetch(url, {
      headers: { Authorization: `Bearer ${accessToken}`, Accept: "application/json" },
    });
  }

  return resp;
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

    const resourceUrl = payloadObj?.body?.resource_url || null;
    const projectIdMatch = resourceUrl ? resourceUrl.match(/\/projects\/([^\/\?\#]+)/) : null;
    const projectId = projectIdMatch ? projectIdMatch[1] : null;

    // 1) events_raw (dedupe)
    const eventRow = {
      received_at: payloadObj.received_at ? new Date(payloadObj.received_at) : new Date(),
      event_id: msg.messageId,
      source: "daylite_webhook",
      entity_type: "project",
      entity_id: projectId,
      action: "update",
      payload: JSON.stringify(payloadObj),
    };

    await bq.dataset(DATASET).table(EVENTS_TABLE).insert([
      { insertId: msg.messageId, json: eventRow },
    ]);

    // 2) fetch project snapshot (no retry storm)
    if (resourceUrl) {
      try {
        const resp = await fetchWithAuth(resourceUrl);
        if (!resp.ok) {
          const t = await resp.text();
          throw new Error(`fetch project failed: ${resp.status} ${t}`);
        }

        const projectJson = await resp.json();
        const projectRow = {
          fetched_at: new Date(),
          project_id: projectId,
          resource_url: resourceUrl,
          project_json: JSON.stringify(projectJson),
        };

        await bq.dataset(DATASET).table(PROJECTS_TABLE).insert([
          { insertId: `${msg.messageId}-project`, json: projectRow },
        ]);
      } catch (e) {
        console.error("project fetch/insert failed", e?.message || e);
      }
    }

    // ✅ altijd 2xx terug → Pub/Sub stopt met retries
    return res.status(204).send();
  } catch (e) {
    console.error("handler failed", e?.message || e);
    return res.status(204).send();
  }
});

app.listen(process.env.PORT || 8080, () => {
  console.log("listening on", process.env.PORT || 8080);
});
