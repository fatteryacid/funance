-- Run this after processing data
-- Removes any that has been processed
DELETE FROM staging_raw_data
WHERE listing_id IN (
    SELECT DISTINCT
        staging_raw_data.listing_id
    FROM staging_raw_data
    INNER JOIN staging_processed_data
        ON  staging_raw_data.listing_id = staging_processed_data.listing_id
);