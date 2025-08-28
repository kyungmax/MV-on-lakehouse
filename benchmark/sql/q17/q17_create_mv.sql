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
  );