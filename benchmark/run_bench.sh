#!/usr/bin/env bash
set -euo pipefail
source ./env.sh

# Create tables and load data using spark-sql and spark-shell
# ${SPARKSQL} -f ./sql/create_tables.sql
# ${SPARKSQL} -f ./sql/load_data.sql
${SPARKSQL} -f ./sql/load_refresh_function_data.sql

# QUERIES=(q1 q3 q4 q5 q6 q7 q9 q10 q12 q18 q19 q21 q22)
QUERIES=(q4 q7 q9 q21)
MODES=(baseline full ivm)
# MODES=(ivm)

summary="${LOGDIR}/summary.csv"
echo "scale,query,mode,e2e_ms" > "$summary"

set_mode_sql() {  # $1=mode
  case "$1" in
    baseline) echo "SET spark.sql.MV.rewrite.enabled=false; SET spark.sql.MV.full.refresh=false; SET mode=baseline;" ;;
    full)     echo "SET spark.sql.MV.rewrite.enabled=true;  SET spark.sql.MV.full.refresh=true; SET mode=full;"  ;;
    ivm)      echo "SET spark.sql.MV.rewrite.enabled=true;  SET spark.sql.MV.full.refresh=false; SET mode=ivm;" ;;
  esac
}

# Reload baseline for orders/lineitem (drop & recreate)
reload_orders_lineitem() {
  echo "[reload] Dropping tpch.orders & tpch.lineitem (PURGE)" >&2
  ${SPARKSQL} -e "DROP TABLE IF EXISTS my_rest.tpch.lineitem PURGE; DROP TABLE IF EXISTS my_rest.tpch.orders PURGE;" || {
    echo "[ERROR] drop orders/lineitem failed" >&2; return 1; }
  if [[ -f ./sql/create_tables.sql ]]; then
    echo "[reload] Running ./sql/create_tables.sql" >&2
    ${SPARKSQL} -f ./sql/create_tables.sql || { echo "[ERROR] create_tables.sql failed" >&2; return 1; }
  else
    echo "[WARN] ./sql/create_tables.sql not found — ensure tables exist before load" >&2
  fi
  if [[ -f ./sql/load_data.sql ]]; then
    echo "[reload] Running ./sql/load_data.sql" >&2
    ${SPARKSQL} -f ./sql/load_data.sql || { echo "[ERROR] load_data.sql failed" >&2; return 1; }
  else
    echo "[WARN] ./sql/load_data.sql not found — baseline data may be missing" >&2
  fi
}

run_set() {
  local q="$1"
  local mode="$2"
  local createSQL="./sql/${q}/${q}_create_mv.sql"
  local dropSQL="./sql/${q}/${q}_drop_mv.sql"
  local refreshSQL="./sql/${q}/${q}_refresh_mv.sql"
  local mainSQL_1="./sql/${q}/${q}_1.sql"
  local mainSQL_2="./sql/${q}/${q}_2.sql"
  local mainSQL_3="./sql/${q}/${q}_3.sql"
  local log="${LOGDIR}/${SCALE}_${q}_${mode}.log"
  : > "$log"

  # Reload orders & lineitem
  echo "Reloading orders & lineitem ..."
  reload_orders_lineitem

  # Build a single SQL bundle to keep one spark-sql session per mode
  local tmp_sql="${LOGDIR}/__${q}_${mode}.sql"
  : > "$tmp_sql"

  # Always set the mode flags first so the same SELECT behaves differently
  # We also start an internal timer inside Spark to exclude process startup time
  echo "$(set_mode_sql "$mode")" >> "$tmp_sql"
  echo "DROP TABLE IF EXISTS my_rest.tpch._t0;" >> "$tmp_sql"
  echo "CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;" >> "$tmp_sql"

  if [ "$mode" = "baseline" ]; then
    echo "Running in baseline mode (table scan)."
    # Baseline: no MV create/drop, just run main SQL
    cat "$mainSQL_1" >> "$tmp_sql"
    cat "$mainSQL_2" >> "$tmp_sql"
    cat "$mainSQL_3" >> "$tmp_sql"
  elif [ "$mode" = "full" ]; then
    echo "Running in ${mode} mode."
    cat "$dropSQL"   >> "$tmp_sql"
    cat "$createSQL" >> "$tmp_sql"
    cat "$mainSQL_1"   >> "$tmp_sql"
    cat "$mainSQL_2"   >> "$tmp_sql"
    cat "$mainSQL_3"   >> "$tmp_sql"
    cat "$dropSQL"   >> "$tmp_sql"
  elif [ "$mode" = "ivm" ]; then
    echo "Running in ${mode} mode."
    cat "$dropSQL"   >> "$tmp_sql"
    cat "$createSQL" >> "$tmp_sql"
    cat "$mainSQL_1"   >> "$tmp_sql"
    cat "$refreshSQL" >> "$tmp_sql"
    cat "$mainSQL_2"   >> "$tmp_sql"
    cat "$refreshSQL" >> "$tmp_sql"
    cat "$mainSQL_3"   >> "$tmp_sql"
    cat "$dropSQL"   >> "$tmp_sql"
  else
    echo "[ERROR] Unknown mode: $mode" >&2
    return 1
  fi

  # Emit a single CSV line from inside Spark: scale,query,mode,e2e_ms then drop table
  echo "SELECT concat('${SCALE},${q},${mode},',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;" >> "$tmp_sql"
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

}

# -------- main --------
for q in "${QUERIES[@]}"; do
  for m in "${MODES[@]}"; do
    echo "[*] ${q} → ${m}"
    run_set "$q" "$m"
  done
done

echo "Done → $summary"
