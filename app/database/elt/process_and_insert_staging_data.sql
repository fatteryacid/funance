-- Processing contains the following:
--   - Convert to correct data types
--   - Separate out concatenated metadata fields
--   - Remove leading and trailing whitespace from strings
INSERT INTO staging_processed_data (
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
WITH 
generate_metadata AS (
    SELECT 
        *,
        LEFT(year_string, 4)                                                AS preprocess_year_string,
        RIGHT(year_string, -4)                                              AS preprocess_model_description,
        SPLIT_PART(listing_location, ',', 1)                                AS preprocess_city,
        SPLIT_PART(listing_location, ',', 2)                                AS preprocess_state,
        REGEXP_REPLACE(price, '[^0-9]+', '', 'g')                           AS preprocess_price,
        REGEXP_REPLACE(mileage, '[^0-9]+', '', 'g')                         AS preprocess_mileage,
        CASE WHEN listing_date LIKE '%days ago%' OR listing_date = 'Yesterday' THEN TRUE ELSE FALSE END   AS listing_date_requires_arithmetic,
        CAST(fetch_ts AS TIMESTAMP)                                         AS preprocess_fetch_ts
    FROM staging_raw_data
),

arithmetic_date_number_days_ago AS (
    SELECT 
        *,
        CAST(SPLIT_PART(listing_date, ' ', 1) AS INTEGER)                   AS number_days
    FROM generate_metadata
    WHERE 1=1
        AND listing_date_requires_arithmetic
),

arithmetic_date_set AS (
    SELECT
        CAST(preprocess_year_string AS INTEGER) AS model_year,
        make,
        model,
        TRIM(preprocess_model_description)  AS model_description,
        CAST(CASE WHEN preprocess_price = '' THEN NULL ELSE preprocess_price END AS NUMERIC)   AS price,
        CAST(preprocess_mileage AS INTEGER) AS mileage,
        TRIM(preprocess_city)               AS listing_city,
        TRIM(preprocess_state)              AS listing_state,
        CASE 
            WHEN listing_date LIKE '%days ago%' THEN CAST(preprocess_fetch_ts - CAST(number_days || ' ' || 'DAY' AS INTERVAL) AS DATE)  
            WHEN listing_date = 'Yesterday' THEN CAST(preprocess_fetch_ts - INTERVAL '1 DAY' AS DATE)
            ELSE NULL
        END                                 AS listing_date,
        listing_id,
        TRIM(listing_url)                   AS listing_url,
        preprocess_fetch_ts                 AS fetch_ts
    FROM arithmetic_date_number_days_ago
),

convert_date_metadata AS (
    SELECT 
        *,
        CASE
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Jan' THEN '1'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Feb' THEN '2'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Mar' THEN '3'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Apr' THEN '4'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'May' THEN '5'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Jun' THEN '6'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Jul' THEN '7'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Aug' THEN '8'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Sep' THEN '9'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Oct' THEN '10'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Nov' THEN '11'
            WHEN SPLIT_PART(listing_date, ' ', 1) = 'Dec' THEN '12'
            ELSE NULL
        END                                 AS month_number,
        REGEXP_REPLACE(SPLIT_PART(listing_date, ' ', 2), '[^0-9]+', '', 'g')    AS day_number,
        EXTRACT(YEAR FROM preprocess_fetch_ts)         AS year_number
    FROM generate_metadata
    WHERE 1=1
        AND NOT listing_date_requires_arithmetic
),

convert_date_set AS (
    SELECT
        CAST(preprocess_year_string AS INTEGER) AS model_year,
        make,
        model,
        TRIM(preprocess_model_description)  AS model_description,
        CAST(CASE WHEN preprocess_price = '' THEN NULL ELSE preprocess_price END AS NUMERIC)   AS price,
        CAST(preprocess_mileage AS INTEGER) AS mileage,
        TRIM(preprocess_city)               AS listing_city,
        TRIM(preprocess_state)              AS listing_state,
        CAST(year_number || '-' || month_number || '-' || day_number AS DATE)   AS listing_date,
        listing_id,
        TRIM(listing_url)                   AS listing_url,
        preprocess_fetch_ts                 AS fetch_ts
    FROM convert_date_metadata
),

processed_data AS (
    SELECT * FROM arithmetic_date_set
    UNION ALL 
    SELECT * FROM convert_date_set
)

SELECT * 
FROM processed_data
;