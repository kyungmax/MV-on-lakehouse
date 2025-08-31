SET spark.sql.MV.rewrite.enabled=true;  SET spark.sql.MV.full.refresh=true; SET mode=full;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q10mv;
create materialized view my_rest.tpch.q10mv
as
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
      sum(l_extendedprice * (1 - l_discount)) desc;
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
INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;

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


DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);
DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);

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
DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q10mv;
SELECT concat('sf1,q10,full,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
