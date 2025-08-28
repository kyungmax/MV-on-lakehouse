SET spark.sql.MV.rewrite.enabled=false; SET spark.sql.MV.full.refresh=false; SET mode=baseline;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;
INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;


select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  
DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);

DELETE FROM my_rest.tpch.orders
WHERE o_orderkey IN (SELECT okey FROM rf_del);

select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  select
  sum(l_extendedprice * l_discount)
from
  my_rest.tpch.lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;

  SELECT concat('sf1,q6,baseline,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
