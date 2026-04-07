import fs from "node:fs/promises";
import { Firestore } from "@google-cloud/firestore";

const TOKENS_FILE =
  process.env.TOKENS_FILE || `${process.env.HOME}/daylite-str/.secrets/tokens.json`;
const DOC_PATH = process.env.TOKENS_DOC_PATH || "daylite/tokens";

const firestore = new Firestore();

const raw = await fs.readFile(TOKENS_FILE, "utf8");
const t = JSON.parse(raw);

if (!t.access_token || !t.refresh_token) {
  throw new Error("TOKENS_FILE missing access_token/refresh_token");
}

const [col, doc] = DOC_PATH.split("/");
await firestore.collection(col).doc(doc).set({
  access_token: t.access_token,
  refresh_token: t.refresh_token,
  updated_at: t.updated_at || new Date().toISOString(),
});

console.log(`✅ Seeded Firestore doc ${DOC_PATH} from ${TOKENS_FILE}`);
