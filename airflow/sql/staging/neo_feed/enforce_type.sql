SELECT 
    CAST(id AS STRING) AS id,
    CAST(neo_reference_id AS STRING) AS neo_reference_id,
    CAST(name AS STRING) AS name,
    CAST(nasa_jpl_url AS STRING) AS nasa_jpl_url,
    CAST(absolute_magnitude_h AS FLOAT64) AS absolute_magnitude_h,
    CAST(is_potentially_hazardous_asteroid AS BOOLEAN) AS is_potentially_hazardous_asteroid,
    CAST(is_sentry_object AS BOOLEAN) AS is_sentry_object,
    CAST(close_approach_date AS DATE) AS close_approach_date,
    CAST(estimated_diameter_kilometers_estimated_diameter_min AS FLOAT64) AS estimated_diameter_kilometers_estimated_diameter_min,
    CAST(estimated_diameter_kilometers_estimated_diameter_max AS FLOAT64) AS estimated_diameter_kilometers_estimated_diameter_max
FROM `nasa-neows.de_dataset_staging.neo_feed`;
