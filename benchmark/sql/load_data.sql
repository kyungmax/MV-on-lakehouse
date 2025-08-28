-- load_data.sql
-- Spark SQL script to load TPC-H ORDERS and LINEITEM from dbgen .tbl files
-- Assumes spark-sql is started with: --conf spark.sql.variable.substitute=true
-- Set DATA_DIR to the directory containing dbgen outputs (orders.tbl, lineitem.tbl)
-- =========================
-- 1) ORDERS LOAD
-- =========================
-- Read as text to robustly handle trailing '|'
CREATE OR REPLACE TEMP VIEW _orders_raw AS
SELECT value AS line
FROM text.`file:///Users/kyungmin/coding/study/tpch-dbgen/orders.tbl`;

-- Parse & cast
DROP TABLE IF EXISTS _orders_parsed;
CREATE TABLE _orders_parsed AS
SELECT
  CAST(f[0] AS BIGINT)   AS o_orderkey,
  CAST(f[1] AS BIGINT)   AS o_custkey,
  CAST(f[2] AS STRING)   AS o_orderstatus,
  CAST(f[3] AS DOUBLE)   AS o_totalprice,
  CAST(f[4] AS DATE)     AS o_orderdate,
  CAST(f[5] AS STRING)   AS o_orderpriority,
  CAST(f[6] AS STRING)   AS o_clerk,
  CAST(f[7] AS INT)      AS o_shippriority,
  CAST(f[8] AS STRING)   AS o_comment
FROM (
  SELECT split(line, '\\|') AS f
  FROM _orders_raw
) t
WHERE f[0] RLIKE '^[0-9]+';  -- guard against empty/trash lines

-- Insert into Iceberg table
INSERT INTO my_rest.tpch.orders
SELECT * FROM _orders_parsed;

DROP TABLE IF EXISTS _orders_parsed;

-- =========================
-- 2) LINEITEM LOAD
-- =========================
CREATE OR REPLACE TEMP VIEW _line_raw AS
SELECT value AS line
FROM text.`file:///Users/kyungmin/coding/study/tpch-dbgen/lineitem.tbl`;

DROP TABLE IF EXISTS _lineitem_parsed;
CREATE TABLE _lineitem_parsed AS
SELECT
  CAST(f[0]  AS BIGINT)  AS l_orderkey,
  CAST(f[1]  AS BIGINT)  AS l_partkey,
  CAST(f[2]  AS BIGINT)  AS l_suppkey,
  CAST(f[3]  AS INT)     AS l_linenumber,
  CAST(f[4]  AS DOUBLE)  AS l_quantity,
  CAST(f[5]  AS DOUBLE)  AS l_extendedprice,
  CAST(f[6]  AS DOUBLE)  AS l_discount,
  CAST(f[7]  AS DOUBLE)  AS l_tax,
  CAST(f[8]  AS STRING)  AS l_returnflag,
  CAST(f[9]  AS STRING)  AS l_linestatus,
  CAST(f[10] AS DATE)    AS l_shipdate,
  CAST(f[11] AS DATE)    AS l_commitdate,
  CAST(f[12] AS DATE)    AS l_receiptdate,
  CAST(f[13] AS STRING)  AS l_shipinstruct,
  CAST(f[14] AS STRING)  AS l_shipmode,
  CAST(f[15] AS STRING)  AS l_comment
FROM (
  SELECT split(line, '\\|') AS f
  FROM _line_raw
) t
WHERE f[0] RLIKE '^[0-9]+';

INSERT INTO my_rest.tpch.lineitem
SELECT * FROM _lineitem_parsed;

DROP TABLE IF EXISTS _lineitem_parsed;