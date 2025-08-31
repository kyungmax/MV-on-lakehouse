SET spark.sql.MV.rewrite.enabled=false; SET spark.sql.MV.full.refresh=false; SET mode=baseline;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;

SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;

SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;

SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;


-- #2 INSERT (Refresh Function)
INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;
INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;
SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;

SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;

SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;

SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;

DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);
DELETE FROM my_rest.tpch.orders
WHERE o_orderkey IN (SELECT okey FROM rf_del);

SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;

SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;

SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;

SELECT n_name, sum(l_extendedprice * (1 - l_discount))
FROM my_rest.tpch.customer,
     my_rest.tpch.orders,
     my_rest.tpch.lineitem,
     my_rest.tpch.supplier,
     my_rest.tpch.nation,
     my_rest.tpch.region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC;SELECT concat('sf1,q5,baseline,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
