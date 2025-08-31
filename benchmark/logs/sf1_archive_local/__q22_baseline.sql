SET spark.sql.MV.rewrite.enabled=false; SET spark.sql.MV.full.refresh=false; SET mode=baseline;
DROP TABLE IF EXISTS my_rest.tpch._t0;
CREATE TABLE my_rest.tpch._t0 AS SELECT unix_millis(current_timestamp()) AS t0s;
select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

  select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

  select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

  select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

INSERT INTO my_rest.tpch.orders
SELECT * FROM rf_orders;INSERT INTO my_rest.tpch.lineitem
SELECT * FROM rf_lineitem;

select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

  select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

  select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

  select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

DELETE FROM my_rest.tpch.lineitem
WHERE l_orderkey IN (SELECT okey FROM rf_del);DELETE FROM my_rest.tpch.orders
WHERE o_orderkey IN (SELECT okey FROM rf_del);


select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

  select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

  select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

  select
  cntrycode,
  count(*),
  sum(c_acctbal)
from
  (
    select
      substring(c_phone, 1, 2) as cntrycode,
      c_acctbal
    from
      my_rest.tpch.customer
    where
      substring(c_phone, 1, 2) in
        ('13', '31', '23', '29', '30', '18', '17')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          my_rest.tpch.customer
        where
          c_acctbal > 0.00
          and substring(c_phone, 1, 2) in
            ('13', '31', '23', '29', '30', '18', '17')
      )   
      and not exists (
        select
          *
        from
          my_rest.tpch.orders
        where
          o_custkey = c_custkey
      )   
  ) as custsale
group by
  cntrycode
order by
  cntrycode;SELECT concat('sf1,q22,baseline,',(unix_millis(current_timestamp()) - t0.t0s)) AS e2e_csv FROM my_rest.tpch._t0 t0;
EXIT;
