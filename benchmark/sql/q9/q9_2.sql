
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
