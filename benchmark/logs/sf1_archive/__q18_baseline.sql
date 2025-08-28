SET spark.sql.MV.rewrite.enabled=false; SET spark.sql.MV.full.refresh=false; SET mode=baseline;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
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
INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;

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


DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);
DELETE FROM my_rest.tpch.orders
WHERE o_orderkey IN (SELECT okey FROM rf_del);

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
limit 100;SELECT concat('sf1,q18,baseline,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
