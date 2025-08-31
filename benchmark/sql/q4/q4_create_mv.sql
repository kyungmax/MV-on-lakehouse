create materialized view my_rest.tpch.q4mv
as
SELECT o_orderpriority, COUNT(*)
FROM my_rest.tpch.orders
WHERE o_orderdate >= DATE '1993-07-01'
  AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
  AND EXISTS (
    SELECT *
    FROM my_rest.tpch.lineitem
    WHERE l_orderkey = o_orderkey
      AND l_commitdate < l_receiptdate
  )
GROUP BY o_orderpriority
ORDER BY o_orderpriority;