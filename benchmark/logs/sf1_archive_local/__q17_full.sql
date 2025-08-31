SET spark.sql.MV.rewrite.enabled=true;  SET spark.sql.MV.full.refresh=true; SET mode=full;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q17mv;
create materialized view my_rest.tpch.q17mv
as
select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

  select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

  select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

  select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;
INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;

select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

  select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

  select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

  select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);
DELETE FROM my_rest.tpch.orders
WHERE o_orderkey IN (SELECT okey FROM rf_del);

select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

  select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

  select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );

  select
  sum(l_extendedprice) / 7.0
from
  my_rest.tpch.lineitem,
  my_rest.tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      my_rest.tpch.lineitem
    where
      l_partkey = p_partkey
  );DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q17mv;
SELECT concat('sf1,q17,full,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
