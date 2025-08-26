DROP TABLE IF EXISTS rf_lineitem;
DROP TABLE IF EXISTS rf_orders;
DROP TABLE IF EXISTS rf_del;

-- lineitem

CREATE OR REPLACE TEMP VIEW rf_line_raw AS
SELECT value AS line
FROM text.`file:///Users/kyungmin/coding/study/tpch-dbgen/lineitem.tbl.u1`;

CREATE TABLE rf_lineitem AS
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
  FROM rf_line_raw
) t
WHERE f[0] RLIKE '^[0-9]+$';

-- orders
CREATE OR REPLACE TEMP VIEW rf_orders_raw AS
SELECT value AS line
FROM text.`file:///Users/kyungmin/coding/study/tpch-dbgen/orders.tbl.u1`;

CREATE TABLE rf_orders AS
SELECT
  CAST(f[0] AS BIGINT)  AS o_orderkey,
  CAST(f[1] AS BIGINT)  AS o_custkey,
  CAST(f[2] AS STRING)  AS o_orderstatus,
  CAST(f[3] AS DOUBLE)  AS o_totalprice,
  CAST(f[4] AS DATE)    AS o_orderdate,
  CAST(f[5] AS STRING)  AS o_orderpriority,
  CAST(f[6] AS STRING)  AS o_clerk,
  CAST(f[7] AS INT)     AS o_shippriority,
  CAST(f[8] AS STRING)  AS o_comment
FROM (
  SELECT split(line, '\\|') AS f
  FROM rf_orders_raw
) t
WHERE f[0] RLIKE '^[0-9]+$';

CREATE TABLE rf_del AS
SELECT CAST(regexp_extract(value, '(\\d+)', 1) AS BIGINT) AS okey
FROM text.`file:///Users/kyungmin/coding/study/tpch-dbgen/delete.1`
WHERE value RLIKE '^[0-9]+';