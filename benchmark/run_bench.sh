#!/usr/bin/env bash
set -euo pipefail
source ./env.sh

# Create tables and load data using spark-sql and spark-shell
# ${SPARKSQL} -f ./sql/create_tables.sql
# ${SPARKSQL} -f ./sql/load_data.sql

# QUERIES=(q1 q3 q4 q6 q10 q12 q18 q19 q20 q21)
QUERIES=(q1)
# MODES=(baseline full ivm)
MODES=(baseline full ivm)

summary="${LOGDIR}/summary.csv"
echo "scale,query,mode,e2e_ms" > "$summary"

set_mode_sql() {  # $1=mode
  case "$1" in
    baseline) echo "SET spark.sql.MV.rewrite.enabled=false; SET spark.sql.MV.full.refresh=false;" ;;
    full)     echo "SET spark.sql.MV.rewrite.enabled=true;  SET spark.sql.MV.full.refresh=true;"  ;;
    ivm)      echo "SET spark.sql.MV.rewrite.enabled=true;  SET spark.sql.MV.full.refresh=false;" ;;
  esac
}

# Return space-separated tables to rollback for a given query id
get_affected_tables() {  # $1=qid (e.g., q1)
  case "$1" in
    q1)  echo "lineitem" ;;
    q3)  echo "lineitem" ;;
    q4)  echo "orders lineitem" ;;
    q6)  echo "lineitem" ;;
    q10) echo "orders" ;;
    q12) echo "lineitem" ;;
    q18) echo "orders" ;;
    q19) echo "lineitem" ;;
    q20) echo "lineitem" ;;
    q21) echo "orders" ;;
    *)   echo "" ;;
  esac
}

# Get latest snapshot id for a table
ss_get_latest() {  # $1=table
  ${SPARKSQL} -e "SELECT snapshot_id FROM ${CATALOG}.${DB}.$1.snapshots ORDER BY committed_at DESC LIMIT 1; EXIT;" | tail -n1
}

# Rollback a table to a snapshot id
ss_rollback() {    # $1=table  $2=snapshot_id
  ${SPARKSQL} -e "CALL ${CATALOG}.system.rollback_to_snapshot(table => '${DB}.$1', snapshot_id => $2); EXIT;" >/dev/null
}

run_set() {
  local q="$1"
  local mode="$2"
  local createSQL="./sql/${q}/${q}_create_mv.sql"
  local dropSQL="./sql/${q}/${q}_drop_mv.sql"
  local mainSQL="./sql/${q}/${q}.sql"
  local log="${LOGDIR}/${SCALE}_${q}_${mode}.log"
  : > "$log"

  # Capture snapshots for rollback (per-query static list)
  local AFFECTED
  AFFECTED="$(get_affected_tables "$q")"
  local SNAP_TABLES=()
  local SNAP_IDS=()
  for t in $AFFECTED; do
    SNAP_TABLES+=("$t")
    SNAP_IDS+=("$(ss_get_latest "$t")")
  done

  # Build a single SQL bundle to keep one spark-sql session per mode
  local tmp_sql="${LOGDIR}/__${q}_${mode}.sql"
  : > "$tmp_sql"

  # Always set the mode flags first so the same SELECT behaves differently
  # We also start an internal timer inside Spark to exclude process startup time
  echo "$(set_mode_sql "$mode")" >> "$tmp_sql"
  echo "CREATE TABLE _t0 AS SELECT unix_timestamp() AS t0s;" >> "$tmp_sql"
: '
  if [ "$mode" = "baseline" ]; then
    echo "Running in baseline mode (table scan)."
    # Baseline: no MV create/drop, just run main SQL
    cat "$mainSQL" >> "$tmp_sql"
  elif [ "$mode" = "full" ]; then
    echo "Running in full refresh mode."
    cat "$createSQL" >> "$tmp_sql"
    cat "$mainSQL"   >> "$tmp_sql"
    cat "$dropSQL"   >> "$tmp_sql"
  elif [ "$mode" = "ivm" ]; then
    echo "Running in IVM mode."
    cat "$createSQL" >> "$tmp_sql"
    cat "$mainSQL"   >> "$tmp_sql"
    cat "$dropSQL"   >> "$tmp_sql"
  else
    echo "[ERROR] Unknown mode: $mode" >&2
    return 1
  fi
'
  # Emit a single CSV line from inside Spark: scale,query,mode,e2e_ms then drop table
  echo "SELECT concat('${SCALE},${q},${mode},', CAST((unix_timestamp() - t0.t0s) * 1000 AS BIGINT)) AS e2e_csv FROM _t0 t0;" >> "$tmp_sql"
  echo "DROP TABLE _t0;" >> "$tmp_sql"
  echo "EXIT;" >> "$tmp_sql"

  # Run exactly one spark-sql process per mode; timing excludes startup because it is measured inside Spark
  ${SPARKSQL} -f "$tmp_sql" > "$log" 2>&1

  # Parse the CSV line and append to summary
  local e2e_line
  e2e_line=$(grep -E "^${SCALE},${q},${mode},[0-9]+$" "$log" | tail -n1 || true)
  if [[ -z "$e2e_line" ]]; then
    echo "[WARN] ${q}/${mode} e2e_ms not found in log: $log" >&2
  else
    echo "$e2e_line" >> "$summary"
  fi

  # Rollback all affected tables to their original snapshots
  local i
  for i in "${!SNAP_TABLES[@]}"; do
    ss_rollback "${SNAP_TABLES[$i]}" "${SNAP_IDS[$i]}"
  done
}

# -------- main --------
for q in "${QUERIES[@]}"; do
  for m in "${MODES[@]}"; do
    echo "[*] ${q} → ${m}"
    run_set "$q" "$m"
  done
done

echo "Done → $summary"
