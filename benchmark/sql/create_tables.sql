-- spark-sql에서 실행
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

-- CUSTOMER (identity)
DROP TABLE IF EXISTS my_rest.tpch.customer;

CREATE TABLE
    my_rest.tpch.customer (
        c_custkey BIGINT,
        c_name STRING,
        c_address STRING,
        c_nationkey BIGINT,
        c_phone STRING,
        c_acctbal DOUBLE,
        c_mktsegment STRING,
        c_comment STRING
    ) USING iceberg PARTITIONED BY (identity (c_nationkey)) TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.delete-file.mode' = 'equality-delete',
        'write.update.mode' = 'merge-on-read'
    );

-- PART (identity)
DROP TABLE IF EXISTS my_rest.tpch.part;

CREATE TABLE
    my_rest.tpch.part (
        p_partkey BIGINT,
        p_name STRING,
        p_mfgr STRING,
        p_brand STRING,
        p_type STRING,
        p_size INT,
        p_container STRING,
        p_retailprice DOUBLE,
        p_comment STRING
    ) USING iceberg PARTITIONED BY (identity (p_type)) TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.delete-file.mode' = 'equality-delete',
        'write.update.mode' = 'merge-on-read'
    );

-- PARTSUPP (identity) -- 파티션 안함
DROP TABLE IF EXISTS my_rest.tpch.partsupp;

CREATE TABLE
    my_rest.tpch.partsupp (
        ps_partkey BIGINT,
        ps_suppkey BIGINT,
        ps_availqty INT,
        ps_supplycost DOUBLE,
        ps_comment STRING
    ) USING iceberg TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.delete-file.mode' = 'equality-delete',
        'write.update.mode' = 'merge-on-read'
    );

-- SUPPLIER / NATION / REGION (identity)
DROP TABLE IF EXISTS my_rest.tpch.supplier;

CREATE TABLE
    my_rest.tpch.supplier (
        s_suppkey BIGINT,
        s_name STRING,
        s_address STRING,
        s_nationkey BIGINT,
        s_phone STRING,
        s_acctbal DOUBLE,
        s_comment STRING
    ) USING iceberg PARTITIONED BY (identity (s_nationkey)) TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.delete-file.mode' = 'equality-delete',
        'write.update.mode' = 'merge-on-read'
    );

DROP TABLE IF EXISTS my_rest.tpch.nation;

CREATE TABLE
    my_rest.tpch.nation (
        n_nationkey BIGINT,
        n_name STRING,
        n_regionkey BIGINT,
        n_comment STRING
    ) USING iceberg PARTITIONED BY (identity (n_regionkey)) TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.delete-file.mode' = 'equality-delete',
        'write.update.mode' = 'merge-on-read'
    );

DROP TABLE IF EXISTS my_rest.tpch.region;

CREATE TABLE
    my_rest.tpch.region (
        r_regionkey BIGINT,
        r_name STRING,
        r_comment STRING
    ) USING iceberg PARTITIONED BY (identity (r_regionkey)) TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.delete-file.mode' = 'equality-delete',
        'write.update.mode' = 'merge-on-read'
    );

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q1mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q2mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q3mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q4mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q5mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q6mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q7mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q8mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q9mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q10mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q11mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q12mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q13mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q14mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q15mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q16mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q17mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q18mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q19mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q20mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q21mv;

DROP MATERIALIZED VIEW IF EXISTS my_rest.tpch.q22mv;