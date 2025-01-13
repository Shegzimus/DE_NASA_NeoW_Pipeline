-- First injection needs CREATE keyword
CREATE OR REPLACE TABLE `nasa-neows.de_dataset_warehouse.neo_feed_cleaned` AS
SELECT *
FROM `nasa-neows.de_dataset_staging.neo_feed`
WHERE id IS NOT NULL
  AND name IS NOT NULL
  AND close_approach_date IS NOT NULL;






