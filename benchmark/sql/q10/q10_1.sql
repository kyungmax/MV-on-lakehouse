select
      c_custkey,
      c_name,
      sum(l_extendedprice * (1 - l_discount)),
      c_acctbal,
      n_name,
      c_address,
      c_phone,
      c_comment
    from
      my_rest.tpch.customer,
      my_rest.tpch.orders,
      my_rest.tpch.lineitem,
      my_rest.tpch.nation
    where
      c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate >= date '1993-10-01'
      and o_orderdate < date '1993-10-01' + interval '3' month
      and l_returnflag = 'R'
      and c_nationkey = n_nationkey
    group by
      c_custkey,
      c_name,
      c_acctbal,
      c_phone,
      n_name,
      c_address,
      c_comment
    order by
      sum(l_extendedprice * (1 - l_discount)) desc
limit 20;


select
      c_custkey,
      c_name,
      sum(l_extendedprice * (1 - l_discount)),
      c_acctbal,
      n_name,
      c_address,
      c_phone,
      c_comment
    from
      my_rest.tpch.customer,
      my_rest.tpch.orders,
      my_rest.tpch.lineitem,
      my_rest.tpch.nation
    where
      c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate >= date '1993-10-01'
      and o_orderdate < date '1993-10-01' + interval '3' month
      and l_returnflag = 'R'
      and c_nationkey = n_nationkey
    group by
      c_custkey,
      c_name,
      c_acctbal,
      c_phone,
      n_name,
      c_address,
      c_comment
    order by
      sum(l_extendedprice * (1 - l_discount)) desc
limit 20;


select
      c_custkey,
      c_name,
      sum(l_extendedprice * (1 - l_discount)),
      c_acctbal,
      n_name,
      c_address,
      c_phone,
      c_comment
    from
      my_rest.tpch.customer,
      my_rest.tpch.orders,
      my_rest.tpch.lineitem,
      my_rest.tpch.nation
    where
      c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate >= date '1993-10-01'
      and o_orderdate < date '1993-10-01' + interval '3' month
      and l_returnflag = 'R'
      and c_nationkey = n_nationkey
    group by
      c_custkey,
      c_name,
      c_acctbal,
      c_phone,
      n_name,
      c_address,
      c_comment
    order by
      sum(l_extendedprice * (1 - l_discount)) desc
limit 20;


select
      c_custkey,
      c_name,
      sum(l_extendedprice * (1 - l_discount)),
      c_acctbal,
      n_name,
      c_address,
      c_phone,
      c_comment
    from
      my_rest.tpch.customer,
      my_rest.tpch.orders,
      my_rest.tpch.lineitem,
      my_rest.tpch.nation
    where
      c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate >= date '1993-10-01'
      and o_orderdate < date '1993-10-01' + interval '3' month
      and l_returnflag = 'R'
      and c_nationkey = n_nationkey
    group by
      c_custkey,
      c_name,
      c_acctbal,
      c_phone,
      n_name,
      c_address,
      c_comment
    order by
      sum(l_extendedprice * (1 - l_discount)) desc
limit 20;



INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;