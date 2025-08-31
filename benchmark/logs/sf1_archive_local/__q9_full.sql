SET spark.sql.MV.rewrite.enabled=true;  SET spark.sql.MV.full.refresh=true; SET mode=full;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q9mv;
create materialized view my_rest.tpch.q9mv
as
select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

  select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

  select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

  select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;


INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;
INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;

select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

  select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

  select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

  select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);

DELETE FROM my_rest.tpch.orders
WHERE o_orderkey IN (SELECT okey FROM rf_del);

select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

  select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

  select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

  select
  nation,
  o_year,
  sum(amount)
from
  (
    select
      n_name as nation,
      extract(year from o_orderdate) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
 my_rest.tpch.part,
       my_rest.tpch.supplier,
       my_rest.tpch.lineitem,
       my_rest.tpch.partsupp,
       my_rest.tpch.orders,
       my_rest.tpch.nation
    where
      s_suppkey = l_suppkey
      and ps_suppkey = l_suppkey
      and ps_partkey = l_partkey
      and p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and s_nationkey = n_nationkey
      and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q9mv;
SELECT concat('sf1,q9,full,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
