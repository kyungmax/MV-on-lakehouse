SET spark.sql.MV.rewrite.enabled=false; SET spark.sql.MV.full.refresh=false;
CREATE TABLE _t0 AS SELECT unix_timestamp() AS t0s;
SELECT concat('sf1,q1,baseline,', CAST((unix_timestamp() - t0.t0s) * 1000 AS BIGINT)) AS e2e_csv FROM _t0 t0;
DROP TABLE _t0;
EXIT;
