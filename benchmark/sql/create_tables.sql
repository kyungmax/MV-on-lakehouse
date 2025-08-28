-- LINEITEM
DROP TABLE IF EXISTS my_rest.tpch.lineitem;

CREATE TABLE
    my_rest.tpch.lineitem (
        l_orderkey BIGINT,
        l_partkey BIGINT,
        l_suppkey BIGINT,
        l_linenumber INT,
        l_quantity DOUBLE,
        l_extendedprice DOUBLE,
        l_discount DOUBLE,
        l_tax DOUBLE,
        l_returnflag STRING,
        l_linestatus STRING,
        l_shipdate DATE,
        l_commitdate DATE,
        l_receiptdate DATE,
        l_shipinstruct STRING,
        l_shipmode STRING,
        l_comment STRING
    ) USING iceberg PARTITIONED BY (month (l_shipdate)) TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.delete-file.mode' = 'equality-delete',
        'write.update.mode' = 'merge-on-read'
    );

-- ORDERS
DROP TABLE IF EXISTS my_rest.tpch.orders;

CREATE TABLE
    my_rest.tpch.orders (
        o_orderkey BIGINT,
        o_custkey BIGINT,
        o_orderstatus STRING,
        o_totalprice DOUBLE,
        o_orderdate DATE,
        o_orderpriority STRING,
        o_clerk STRING,
        o_shippriority INT,
        o_comment STRING
    ) USING iceberg PARTITIONED BY (month (o_orderdate)) TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.delete-file.mode' = 'equality-delete',
        'write.update.mode' = 'merge-on-read'
    );