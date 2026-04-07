#!/usr/bin/env bash
set -euo pipefail

# ---- Settings
LOOKBACK_HOURS="${LOOKBACK_HOURS:-3}"
TOPIC="${TOPIC:-daylite-backfill}"
DATASET="${DATASET:-daylite_db}"
EVENTS_TABLE="${EVENTS_TABLE:-worker_events_raw}"
RATE_SLEEP="${RATE_SLEEP:-0.20}"

# New: controls
WINDOW_TS_COL="${WINDOW_TS_COL:-received_at}"   # received_at | fetched_at | etc
DRY_RUN="${DRY_RUN:-0}"                         # 1 = do not publish, just report
STALE_HOURS="${STALE_HOURS:-24}"                # stop if max timestamp is older than this

# ---- Guards
: "${DATASET:?DATASET is empty}"
: "${EVENTS_TABLE:?EVENTS_TABLE is empty}"

PROJECT_ID="daylite-str"
TABLE="${PROJECT_ID}.${DATASET}.${EVENTS_TABLE}"

# ---- Logging
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_FILE="/tmp/backfill_last3h_${RUN_ID}.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "RUN_ID: $RUN_ID"
echo "TABLE:  $TABLE"
echo "WINDOW_TS_COL: $WINDOW_TS_COL"
echo "DRY_RUN: $DRY_RUN"
echo "RATE_SLEEP: $RATE_SLEEP"

# ---- Helpers
csv_last_field() { tail -n 1 | tr -d '\r'; }

# ---- Window based on latest available timestamp (handles ingestion lag)
END_UTC="$(
  bq query --quiet --use_legacy_sql=false --format=csv \
  "SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(${WINDOW_TS_COL})) AS end_utc
   FROM \`$TABLE\`" \
  2>/dev/null | tail -n +2 | tr -d '\r'
)"

if [[ -z "$END_UTC" ]]; then
  echo "ERROR: END_UTC empty. MAX(${WINDOW_TS_COL}) is NULL or query failed."
  exit 1
fi

END_EPOCH="$(date -u -d "$END_UTC" +%s)"
START_EPOCH="$((END_EPOCH - LOOKBACK_HOURS * 3600))"
START_UTC="$(date -u -d "@$START_EPOCH" +%Y-%m-%dT%H:%M:%SZ)"

# ---- Stale guard
NOW_EPOCH="$(date -u +%s)"
AGE_SEC="$((NOW_EPOCH - END_EPOCH))"
STALE_SEC="$((STALE_HOURS * 3600))"
if (( AGE_SEC > STALE_SEC )); then
  echo "ERROR: Latest ${WINDOW_TS_COL} is stale."
  echo "  max(${WINDOW_TS_COL}) UTC: $END_UTC"
  echo "  age_hours: $((AGE_SEC / 3600))  (threshold: $STALE_HOURS)"
  exit 1
fi

echo "Window UTC: $START_UTC -> $END_UTC"
echo "Window AMS: $(TZ=Europe/Amsterdam date -d "@$START_EPOCH" '+%Y-%m-%d %H:%M:%S') -> $(TZ=Europe/Amsterdam date -d "$END_UTC" '+%Y-%m-%d %H:%M:%S')"

# ---- Get entity types in window
OUT_CSV=/tmp/entity_types_last_window.csv

bq query --quiet --use_legacy_sql=false --format=csv "
SELECT DISTINCT entity_type
FROM \`$TABLE\`
WHERE ${WINDOW_TS_COL} BETWEEN TIMESTAMP('${START_UTC}') AND TIMESTAMP('${END_UTC}')
  AND entity_type IS NOT NULL
  AND entity_type != ''
ORDER BY entity_type
" | tail -n +2 > "$OUT_CSV"

echo "Entity types in window:"
if [[ -s "$OUT_CSV" ]]; then
  cat "$OUT_CSV"
else
  echo "(none)"
fi

# ---- Summary counters
TYPES_TOTAL=0
TYPES_WITH_EVENTS=0
TOTAL_EVENTS_ALL=0
TOTAL_DISTINCT_IDS_ALL=0
TOTAL_NEEDED_ALL=0
TOTAL_PUBLISHED_ALL=0
TOTAL_SKIPPED_NO_CHANGELOG=0

# ---- Loop entity types
while IFS= read -r T; do
  [[ -z "$T" ]] && continue
  TYPES_TOTAL=$((TYPES_TOTAL + 1))

  # Only proceed if changelog table exists
  if ! bq show --format=prettyjson "${PROJECT_ID}:${DATASET}.${T}_changelog" >/dev/null 2>&1; then
    echo "SKIP (no changelog table): $T"
    TOTAL_SKIPPED_NO_CHANGELOG=$((TOTAL_SKIPPED_NO_CHANGELOG + 1))
    continue
  fi

  # Quick window stats (progress)
  STATS="$(
    bq query --quiet --use_legacy_sql=false --format=csv "
      SELECT
        COUNT(*) AS total_events,
        COUNT(DISTINCT CAST(entity_id AS STRING)) AS distinct_entity_ids
      FROM \`${PROJECT_ID}.${DATASET}.${EVENTS_TABLE}\`
      WHERE entity_type='${T}'
        AND ${WINDOW_TS_COL} BETWEEN TIMESTAMP('${START_UTC}') AND TIMESTAMP('${END_UTC}')
    " | csv_last_field
  )"

  TOTAL_EVENTS="$(echo "$STATS" | cut -d, -f1)"
  DISTINCT_IDS="$(echo "$STATS" | cut -d, -f2)"

  echo "WINDOW ${T}: events=${TOTAL_EVENTS} distinct_ids=${DISTINCT_IDS}"

  TOTAL_EVENTS_ALL=$((TOTAL_EVENTS_ALL + TOTAL_EVENTS))
  TOTAL_DISTINCT_IDS_ALL=$((TOTAL_DISTINCT_IDS_ALL + DISTINCT_IDS))

  [[ "${TOTAL_EVENTS}" == "0" ]] && continue
  TYPES_WITH_EVENTS=$((TYPES_WITH_EVENTS + 1))

  # Build needed table
  bq query --quiet --use_legacy_sql=false "
  CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET}.backfill_${T}_needed\` AS
  WITH e AS (
    SELECT
      CAST(entity_id AS STRING) AS entity_id,
      MAX(${WINDOW_TS_COL}) AS last_event_at,
      ANY_VALUE(resource_url) AS resource_url
    FROM \`${PROJECT_ID}.${DATASET}.${EVENTS_TABLE}\`
    WHERE entity_type='${T}'
      AND ${WINDOW_TS_COL} BETWEEN TIMESTAMP('${START_UTC}') AND TIMESTAMP('${END_UTC}')
    GROUP BY CAST(entity_id AS STRING)
  ),
  f AS (
    SELECT
      CAST(entity_id AS STRING) AS entity_id,
      MAX(fetched_at) AS last_fetch_at
    FROM \`${PROJECT_ID}.${DATASET}.${T}_changelog\`
    WHERE entity_type='${T}'
    GROUP BY CAST(entity_id AS STRING)
  )
  SELECT
    '${T}' AS entity_type,
    e.entity_id,
    e.last_event_at AS last_event_at,
    e.resource_url
  FROM e
  LEFT JOIN f USING (entity_id)
  WHERE f.last_fetch_at IS NULL OR f.last_fetch_at < e.last_event_at;
  "

  COUNT="$(
    bq query --quiet --use_legacy_sql=false --format=csv "
      SELECT COUNT(*) AS c
      FROM \`${PROJECT_ID}.${DATASET}.backfill_${T}_needed\`
    " | csv_last_field
  )"

  echo "NEEDED ${T}: ${COUNT}"
  TOTAL_NEEDED_ALL=$((TOTAL_NEEDED_ALL + COUNT))

  [[ "$COUNT" == "0" ]] && continue

  # Dump rows
  bq query --quiet --use_legacy_sql=false --format=csv "
    SELECT entity_type, resource_url, entity_id
    FROM \`${PROJECT_ID}.${DATASET}.backfill_${T}_needed\`
  " | tail -n +2 > "/tmp/backfill_${T}.csv"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "DRY_RUN=1, not publishing ${T} messages."
    continue
  fi

  # Publish
  PUBLISHED_T=0
  while IFS=, read -r entity_type resource_url entity_id; do
    [[ -z "${entity_type}" ]] && continue
    gcloud pubsub topics publish "${TOPIC}" \
      --message "{\"entity_type\":\"$entity_type\",\"resource_url\":\"$resource_url\",\"entity_id\":\"$entity_id\",\"source\":\"backfill\"}" \
      >/dev/null
    PUBLISHED_T=$((PUBLISHED_T + 1))
    sleep "$RATE_SLEEP"
  done < "/tmp/backfill_${T}.csv"

  echo "PUBLISHED ${T}: ${PUBLISHED_T}"
  TOTAL_PUBLISHED_ALL=$((TOTAL_PUBLISHED_ALL + PUBLISHED_T))

done < "$OUT_CSV"

# ---- Summary
echo "----- SUMMARY -----"
echo "types_total:             ${TYPES_TOTAL}"
echo "types_with_events:       ${TYPES_WITH_EVENTS}"
echo "skipped_no_changelog:    ${TOTAL_SKIPPED_NO_CHANGELOG}"
echo "events_total:            ${TOTAL_EVENTS_ALL}"
echo "distinct_ids_total:      ${TOTAL_DISTINCT_IDS_ALL}"
echo "needed_total:            ${TOTAL_NEEDED_ALL}"
echo "published_total:         ${TOTAL_PUBLISHED_ALL}"
echo "log_file:                ${LOG_FILE}"