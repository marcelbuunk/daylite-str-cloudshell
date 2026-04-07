import express from "express";
import { Firestore } from "@google-cloud/firestore";

console.log("DAYLITE-AUTH VERSION: token+refresh queryparam 2026-02-10");

const app = express();
app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: true, limit: "1mb" })); // <-- toevoegen


const PORT = process.env.PORT || 8080;
const db = new Firestore();

const TOKENS_DOC_PATH = process.env.TOKENS_DOC_PATH || "daylite/tokens";

// Docs: refresh via query param ?refresh_token=... (niet via Authorization header)
const PERSONAL_REFRESH_URL = "https://api.marketcircle.net/v1/personal_token/refresh_token";

// Access tokens expire ~1 hour → refresh iets eerder (55 min)
const REFRESH_AFTER_MINUTES = Number(process.env.REFRESH_AFTER_MINUTES || 55);

function tokenDocRef() {
  return db.doc(TOKENS_DOC_PATH); // e.g. "daylite/tokens"
}

async function readTokens() {
  const snap = await tokenDocRef().get();
  return snap.exists ? snap.data() : null;
}

async function writeTokens(tokens) {
  await tokenDocRef().set(tokens, { merge: true });
}

function shouldRefresh(updatedAtIso) {
  if (!updatedAtIso) return true;
  const t = new Date(updatedAtIso).getTime();
  if (!Number.isFinite(t)) return true;
  const ageMs = Date.now() - t;
  return ageMs > REFRESH_AFTER_MINUTES * 60 * 1000;
}

async function refreshNow() {
  const t = await readTokens();
  const refreshToken = t?.refresh_token;
  if (!refreshToken) throw new Error("No refresh_token in Firestore");

  const url = `${PERSONAL_REFRESH_URL}?refresh_token=${encodeURIComponent(refreshToken)}`;

  const resp = await fetch(url, {
    method: "GET",
    headers: { Accept: "application/json" },
  });

  const text = await resp.text();
  if (!resp.ok) throw new Error(`Daylite refresh failed: ${resp.status} ${text}`);

  const json = JSON.parse(text);

  if (!json?.access_token) throw new Error("Refresh returned no access_token");

  // refresh token rotate: nieuwe refresh_token vervangt oude
  const newTokens = {
    access_token: json.access_token,
    refresh_token: json.refresh_token || refreshToken,
    updated_at: new Date().toISOString(),
  };

  await writeTokens(newTokens);
  return newTokens;
}

app.get("/", (_req, res) => res.status(200).send("daylite-auth ok"));

app.get("/__version", (_req, res) =>
  res.status(200).send("DAYLITE-AUTH VERSION: token+refresh queryparam 2026-02-10")
);

app.get("/token", async (_req, res) => {
  try {
    const t = await readTokens();
    if (!t?.refresh_token && !t?.access_token) {
      return res.status(500).json({ ok: false, error: "No tokens in Firestore yet" });
    }

    // Auto-refresh als token oud is
    if (!t?.access_token || shouldRefresh(t?.updated_at)) {
      const newTokens = await refreshNow();
      return res.json({ ok: true, access_token: newTokens.access_token, updated_at: newTokens.updated_at });
    }

    return res.json({ ok: true, access_token: t.access_token, updated_at: t.updated_at || null });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.get("/refresh", async (_req, res) => {
  try {
    const newTokens = await refreshNow();
    return res.json({ ok: true, updated_at: newTokens.updated_at });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.listen(PORT, () => console.log("listening on", PORT));