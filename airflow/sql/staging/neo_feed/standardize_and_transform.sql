SELECT
    id,
    neo_reference_id,
    name,
    nasa_jpl_url,
    absolute_magnitude_h,
    is_potentially_hazardous_asteroid,
    is_sentry_object,
    close_approach_date,
    estimated_diameter_kilometers_estimated_diameter_min AS diameter_km_min,
    estimated_diameter_kilometers_estimated_diameter_max AS diameter_km_max,
    estimated_diameter_meters_estimated_diameter_min AS diameter_m_min,
    estimated_diameter_meters_estimated_diameter_max AS diameter_m_max,
    estimated_diameter_miles_estimated_diameter_min AS diameter_miles_min,
    estimated_diameter_miles_estimated_diameter_max AS diameter_miles_max,
    estimated_diameter_feet_estimated_diameter_min AS diameter_ft_min,
    estimated_diameter_feet_estimated_diameter_max AS diameter_ft_max
FROM `nasa-neows.de_dataset_staging.neo_feed`;