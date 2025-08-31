SET spark.sql.MV.rewrite.enabled=true;  SET spark.sql.MV.full.refresh=true; SET mode=full;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
drop materialized view my_rest.tpch.q3mv;
create materialized view my_rest.tpch.q3mv
as
select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate;select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

-- #2 INSERT (Refresh Function)
INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;
INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);
DELETE FROM my_rest.tpch.orders
WHERE o_orderkey IN (SELECT okey FROM rf_del);

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;

select
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)),
  o_orderdate,
  o_shippriority
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  c_mktsegment = 'BUILDING'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  sum(l_extendedprice * (1 - l_discount)) desc,
  o_orderdate
limit 10;drop materialized view my_rest.tpch.q3mv;
SELECT concat('sf1,q3,full,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
