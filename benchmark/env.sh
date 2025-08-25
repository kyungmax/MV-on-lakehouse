#!/usr/bin/env bash
set -euo pipefail

CATALOG="my_rest"
DB="tpch"
SCALE="sf1"

SPARKSQL="/Users/kyungmin/coding/study/fork-directories/spark/bin/spark-sql \
	--jars \
  /Users/kyungmin/coding/iceberg-spark-runtime-4.0_2.13-c603e5c.dirty.jar, \
  /Users/kyungmin/coding/hadoop-common-2.8.3.jar \
  --driver-class-path /Users/kyungmin/coding/iceberg-aws-bundle-df866c5.jar \
  --conf spark.sql.catalog.my_rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.my_rest.uri=http://localhost:8181 \
  --conf spark.sql.catalog.my_rest.warehouse=s3://warehouse \
  --conf spark.sql.catalog.my_rest.s3.endpoint=http://localhost:9000 \
  --conf spark.sql.catalog.my_rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.my_rest.s3.secret-access-key=password \
  --conf spark.sql.catalog.my_rest.s3.path-style-access=true \
  --conf spark.sql.catalog.my_rest.type=rest \
  --conf spark.sql.catalog.my_rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf "spark.driver.extraJavaOptions=-Daws.region=us-east-1" \
  --conf "spark.executor.extraJavaOptions=-Daws.region=us-east-1" \
  --conf spark.executor.memory=8g \
        --conf spark.driver.memory=4g"

SPARKSHELL="/Users/kyungmin/coding/study/fork-directories/spark/bin/spark-shell \
  --jars \
  /Users/kyungmin/coding/iceberg-spark-runtime-4.0_2.13-9155202.dirty.jar \
  /Users/kyungmin/coding/hadoop-common-2.8.3.jar, /Users/kyungmin/coding/hadoop-aws-3.3.6.jar,  \
  --driver-class-path /Users/kyungmin/coding/iceberg-aws-bundle-df866c5.jar \
  --conf spark.driver.extraJavaOptions=-Daws.region=us-east-1 \
  --conf spark.sql.catalog.my_rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.my_rest.uri=http://localhost:8181 \
  --conf spark.sql.catalog.my_rest.warehouse=s3://warehouse \
  --conf spark.sql.catalog.my_rest.s3.endpoint=http://localhost:9000 \
  --conf spark.sql.catalog.my_rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.my_rest.s3.secret-access-key=password \
  --conf spark.sql.catalog.my_rest.s3.path-style-access=true \
  --conf spark.sql.catalog.my_rest.type=rest \
  --conf spark.sql.catalog.my_rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf "spark.driver.extraJavaOptions=-Daws.region=us-east-1" \
  --conf spark.executor.memory=8g \
	--conf spark.driver.memory=4g"

LOGDIR="./logs"
mkdir -p "${LOGDIR}"
