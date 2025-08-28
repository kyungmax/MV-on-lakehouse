SET spark.sql.MV.rewrite.enabled=true;  SET spark.sql.MV.full.refresh=false; SET mode=ivm;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q20mv;
create materialized view my_rest.tpch.q20mv
as
select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

  select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

  select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

  select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

  
INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;
REFRESH MATERIALIZED VIEW my_rest.tpch.q20mv;
INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;
select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

  select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

  select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

  select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;
DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);REFRESH MATERIALIZED VIEW my_rest.tpch.q20mv;
DELETE FROM my_rest.tpch.orders
WHERE o_orderkey IN (SELECT okey FROM rf_del);
select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

  select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

  select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

  select
  s_name,
  s_address
from
    my_rest.tpch.supplier,
    my_rest.tpch.nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
        my_rest.tpch.partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          my_rest.tpch.part
        where
          p_name like 'forest%'
      )   
      and ps_availqty > (
        select
          0.5 * sum(l_quantity)
        from
            my_rest.tpch.lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
      )   
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q20mv;
SELECT concat('sf1,q20,ivm,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
