import { Firestore } from "@google-cloud/firestore";

const DOC_PATH = process.env.TOKENS_DOC_PATH || "daylite/tokens";
const firestore = new Firestore();

const [col, doc] = DOC_PATH.split("/");
const snap = await firestore.collection(col).doc(doc).get();

if (!snap.exists) {
  console.log("❌ doc not found", DOC_PATH);
  process.exit(1);
}

const data = snap.data();
console.log("✅ doc exists:", DOC_PATH);
console.log({
  has_access_token: !!data.access_token,
  has_refresh_token: !!data.refresh_token,
  updated_at: data.updated_at || null,
});
