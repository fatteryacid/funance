INSERT INTO production_car_analysis_set (
    model_year,
    make,
    model,
    vin,
    model_description,
    price,
    mileage,
    listing_city,
    listing_state,
    listing_zip,
    listing_date,
    listing_id,
    listing_url,
    fetch_ts
)
SELECT *
FROM staging_processed_data
WHERE 1=1
    AND CAST(fetch_ts AS DATE) = CURRENT_DATE       -- Thinking of keeping all staging records, we can partition by fetch_ts
;