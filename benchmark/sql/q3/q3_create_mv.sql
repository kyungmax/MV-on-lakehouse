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
  o_orderdate
limit 10;