SET spark.sql.MV.rewrite.enabled=false; SET spark.sql.MV.full.refresh=false; SET mode=baseline;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;

select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;

select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;

select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;

INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;
INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;
select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;

select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;

select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;

select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;


DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);

DELETE FROM my_rest.tpch.orders
WHERE o_orderkey IN (SELECT okey FROM rf_del);
select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;

select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;

select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;

select
        s_name,
        count(*)
from
        my_rest.tpch.supplier,
        my_rest.tpch.lineitem l1,
        my_rest.tpch.orders,
        my_rest.tpch.nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        my_rest.tpch.lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
order by
        count(*) DESC,
        s_name
limit 100;SELECT concat('sf1,q21,baseline,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
