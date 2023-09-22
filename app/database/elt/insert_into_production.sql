INSERT INTO production_car_analysis_set (
    model_year,
    make,
    model,
    model_description,
    price,
    mileage,
    listing_city,
    listing_state,
    listing_date,
    listing_id,
    listing_url,
    fetch_ts
)
SELECT *
FROM staging_processed_data
;