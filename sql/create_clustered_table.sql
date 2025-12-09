CREATE TABLE `jeni-td-bq-migration.mydataset_1.sampletable_1`
(
  customer_id STRING,
  name STRING,
  city STRING,
  updated_date DATE
)
PARTITION BY updated_date
CLUSTER BY city;
