CREATE TABLE `nasa-neows.de_dataset_warehouse.neo_feed_partitioned`
PARTITION BY DATE(close_approach_date)
CLUSTER BY is_potentially_hazardous_asteroid AS
SELECT *
FROM `nasa-neows.de_dataset_warehouse.neo_feed_cleaned`;
