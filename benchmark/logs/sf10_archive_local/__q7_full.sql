SET spark.sql.MV.rewrite.enabled=true;  SET spark.sql.MV.full.refresh=true; SET mode=full;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q7mv;
create materialized view my_rest.tpch.q7mv
as
SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;


INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;
INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;


DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);

DELETE FROM my_rest.tpch.orders
WHERE o_orderkey IN (SELECT okey FROM rf_del);

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;

SELECT supp_nation, cust_nation, l_year, sum(volume)
FROM (
  SELECT n1.n_name as supp_nation,
         n2.n_name as cust_nation,
         EXTRACT(YEAR FROM l_shipdate) as l_year,
         l_extendedprice * (1 - l_discount) as  volume
  FROM my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.orders,
       my_rest.tpch.customer,
       my_rest.tpch.nation n1,
       my_rest.tpch.nation n2
  WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q7mv;
SELECT concat('sf1,q7,full,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
