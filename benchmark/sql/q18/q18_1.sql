select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  o_orderkey in (
    select
      l_orderkey
    from
      my_rest.tpch.lineitem
    group by
      l_orderkey having
        sum(l_quantity) > 300
  )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate
limit 100;

select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  o_orderkey in (
    select
      l_orderkey
    from
      my_rest.tpch.lineitem
    group by
      l_orderkey having
        sum(l_quantity) > 300
  )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate
limit 100;

select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  o_orderkey in (
    select
      l_orderkey
    from
      my_rest.tpch.lineitem
    group by
      l_orderkey having
        sum(l_quantity) > 300
  )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate
limit 100;

select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  my_rest.tpch.customer,
  my_rest.tpch.orders,
  my_rest.tpch.lineitem
where
  o_orderkey in (
    select
      l_orderkey
    from
      my_rest.tpch.lineitem
    group by
      l_orderkey having
        sum(l_quantity) > 300
  )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate
limit 100;


INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;