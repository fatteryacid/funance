-- Run this after processing data
-- Removes any that has been processed
DELETE FROM staging_processed_data
WHERE listing_id IN (
    SELECT DISTINCT
        staging_processed_data.listing_id
    FROM staging_processed_data
    INNER JOIN production_car_analysis_set
        ON  staging_processed_data.listing_id = production_car_analysis_set.listing_id
);