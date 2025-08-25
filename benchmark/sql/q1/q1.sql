-- #1 Select
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity),
  sum(l_extendedprice),
  sum(l_extendedprice * (1 - l_discount)),
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
  avg(l_quantity),
  avg(l_extendedprice),
  avg(l_discount),
  count(*)
from
  my_rest.tpch.lineitem
where
  l_shipdate <= date '1998-12-01' - interval '90' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;

-- #1-2 Select: Successive select query to emphasize MV's role
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity),
  sum(l_extendedprice),
  sum(l_extendedprice * (1 - l_discount)),
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
  avg(l_quantity),
  avg(l_extendedprice),
  avg(l_discount),
  count(*)
from
  my_rest.tpch.lineitem
where
  l_shipdate <= date '1998-12-01' - interval '90' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;

-- #1-3 Select: Successive select query to emphasize MV's role
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity),
  sum(l_extendedprice),
  sum(l_extendedprice * (1 - l_discount)),
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
  avg(l_quantity),
  avg(l_extendedprice),
  avg(l_discount),
  count(*)
from
  my_rest.tpch.lineitem
where
  l_shipdate <= date '1998-12-01' - interval '90' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;

-- #2 Insert lineitem
INSERT INTO my_rest.tpch.lineitem(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
                     l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
				VALUES(0, 0, 0, 1, 30.00, 369.00, 0.10, 0.05, 'N', 'O', DATE '1997-02-05', DATE '2025-08-03', DATE '1997-10-10', 'NONE', 'AIR', 'Sample lineitem'),
				(0, 0, 0, 1, 30.00, 369.00, 0.10, 0.05, 'N', 'O', DATE '1997-02-05', DATE '2025-08-03', DATE '1997-10-10', 'NONE', 'AIR', 'Sample lineitem'),
				(0, 0, 0, 1, 30.00, 369.00, 0.10, 0.05, 'N', 'O', DATE '1997-02-05', DATE '2025-08-03', DATE '1997-10-10', 'NONE', 'AIR', 'Sample lineitem'),
				(0, 0, 0, 1, 30.00, 369.00, 0.10, 0.05, 'N', 'O', DATE '1997-02-05', DATE '2025-08-03', DATE '1997-10-10', 'NONE', 'AIR', 'Sample lineitem'),
				(0, 0, 0, 1, 30.00, 369.00, 0.10, 0.05, 'N', 'O', DATE '1997-02-05', DATE '2025-08-03', DATE '1997-10-10', 'NONE', 'AIR', 'Sample lineitem'),
				(0, 0, 0, 1, 30.00, 369.00, 0.10, 0.05, 'N', 'O', DATE '1997-02-05', DATE '2025-08-03', DATE '1997-10-10', 'NONE', 'AIR', 'Sample lineitem'),
				(0, 0, 0, 1, 30.00, 369.00, 0.10, 0.05, 'N', 'O', DATE '1997-02-05', DATE '2025-08-03', DATE '1997-10-10', 'NONE', 'AIR', 'Sample lineitem'),
				(0, 0, 0, 1, 30.00, 369.00, 0.10, 0.05, 'N', 'O', DATE '1997-02-05', DATE '2025-08-03', DATE '1997-10-10', 'NONE', 'AIR', 'Sample lineitem'),
				(900001, 900001, 900001, 1,         -- l_orderkey, l_partkey, l_suppkey, l_linenumber
				   5.0, 100.0, 0.05, 0.07,            -- l_quantity, l_extendedprice, l_discount, l_tax
			   'N', 'O',                           -- l_returnflag, l_linestatus (예: New/Open)
		   DATE '1998-09-01',                  -- l_shipdate (조건 통과)
	   DATE '1998-09-02', DATE '1998-09-05',
   'NONE','MAIL','test insert for Q1');

-- #3 Select
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity),
  sum(l_extendedprice),
  sum(l_extendedprice * (1 - l_discount)),
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
  avg(l_quantity),
  avg(l_extendedprice),
  avg(l_discount),
  count(*)
from
  my_rest.tpch.lineitem
where
  l_shipdate <= date '1998-12-01' - interval '90' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;

-- #4 Update

-- #5 Select
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity),
  sum(l_extendedprice),
  sum(l_extendedprice * (1 - l_discount)),
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
  avg(l_quantity),
  avg(l_extendedprice),
  avg(l_discount),
  count(*)
from
  my_rest.tpch.lineitem
where
  l_shipdate <= date '1998-12-01' - interval '90' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;
  
-- #6 Delete

-- #7 Select
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity),
  sum(l_extendedprice),
  sum(l_extendedprice * (1 - l_discount)),
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
  avg(l_quantity),
  avg(l_extendedprice),
  avg(l_discount),
  count(*)
from
  my_rest.tpch.lineitem
where
  l_shipdate <= date '1998-12-01' - interval '90' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;

-- #7-2 Select again
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity),
  sum(l_extendedprice),
  sum(l_extendedprice * (1 - l_discount)),
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
  avg(l_quantity),
  avg(l_extendedprice),
  avg(l_discount),
  count(*)
from
  my_rest.tpch.lineitem
where
  l_shipdate <= date '1998-12-01' - interval '90' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;

-- #7-3 Select again
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity),
  sum(l_extendedprice),
  sum(l_extendedprice * (1 - l_discount)),
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
  avg(l_quantity),
  avg(l_extendedprice),
  avg(l_discount),
  count(*)
from
  my_rest.tpch.lineitem
where
  l_shipdate <= date '1998-12-01' - interval '90' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;
