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

  
INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;
