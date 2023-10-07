-- Processing contains the following:
--   - Convert to correct data types
--   - Separate out concatenated metadata fields
--   - Remove leading and trailing whitespace from strings
INSERT INTO staging_processed_data (
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

WITH 
generate_metadata AS (
    -- Create additional metadata to support data transformations
    -- Also create tag to separate dates that require arithmetic VS straight casts
    SELECT 
        *,
        LOWER(make)                                                         AS preprocess_make,
        CASE
            WHEN model LIKE '%RC%' THEN 'rcf'   -- This is a temp fix, will need to reconsider how to control these points in more scalable manner
            WHEN model LIKE '%GS%' THEN 'gsf'
            WHEN model LIKE '%IS%' THEN 'is500'
            ELSE model
        END                                                                 AS preprocess_model,
        SPLIT_PART(_location, ',', 1)                                       AS preprocess_city,
        SPLIT_PART(_location, ',', 2)                                       AS preprocess_state,
        CASE
            WHEN REGEXP_LIKE(location_zipcode, '[^0-9]+', 'i') THEN NULL
            ELSE location_zipcode
        END                                                                 AS preprocess_zip,
        REGEXP_REPLACE(price, '[^0-9]+', '', 'g')                           AS preprocess_price,
        CASE 
            WHEN REGEXP_REPLACE(mileage, '[^0-9]+', '', 'g') = '' THEN NULL
            ELSE REGEXP_REPLACE(mileage, '[^0-9]+', '', 'g')
        END                                                                 AS preprocess_mileage,
        CASE 
            WHEN listing_date LIKE '%days ago%' OR listing_date IN ('Today', 'Yesterday') THEN TRUE 
            ELSE FALSE 
        END                                                                 AS listing_date_requires_arithmetic,
        CAST(fetch_ts AS TIMESTAMP)                                         AS preprocess_fetch_ts
    FROM staging_raw_data
),

arithmetic_date_number_days_ago AS (
    -- Parse out integer to perform date arithmetic
    SELECT 
        *,
        CASE 
            WHEN listing_date LIKE '%days ago%' THEN CAST(SPLIT_PART(listing_date, ' ', 1) AS INTEGER)
            WHEN listing_date = 'Yesterday' THEN 1
            WHEN listing_date = 'Today' THEN 0
        END                                                                 AS number_days
    FROM generate_metadata
    WHERE 1=1
        AND listing_date_requires_arithmetic
),

arithmetic_date_set AS (
    -- Final set for arithmetic-supportable data points
    SELECT
        CAST(_year AS INTEGER)                                                                  AS model_year,
        preprocess_make                                                                         AS make,
        preprocess_model                                                                        AS model,
        vin,
        TRIM(details)                                                                           AS model_description,
        CAST(CASE WHEN preprocess_price = '' THEN NULL ELSE preprocess_price END AS NUMERIC)    AS price,
        CAST(preprocess_mileage AS INTEGER)                                                     AS mileage,
        TRIM(preprocess_city)                                                                   AS listing_city,
        TRIM(preprocess_state)                                                                  AS listing_state,
        CAST(TRIM(preprocess_zip) AS INTEGER)                                                   AS listing_zip,
        CAST(preprocess_fetch_ts - CAST(number_days || ' ' || 'DAY' AS INTERVAL) AS DATE)       AS listing_date,
        listing_id,
        TRIM(_url)                                                                              AS listing_url,
        preprocess_fetch_ts                                                                     AS fetch_ts
    FROM arithmetic_date_number_days_ago
),

convert_date_metadata AS (
    -- Parse out different date parts for later assembly
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
        END                                                                     AS month_number,
        REGEXP_REPLACE(SPLIT_PART(listing_date, ' ', 2), '[^0-9]+', '', 'g')    AS day_number,
        EXTRACT(YEAR FROM preprocess_fetch_ts)                                  AS year_number
    FROM generate_metadata
    WHERE 1=1
        AND NOT listing_date_requires_arithmetic
),

convert_date_set AS (
    -- Final set for data-casted data points
    SELECT
        CAST(_year AS INTEGER)                                                                  AS model_year,
        preprocess_make                                                                         AS make,
        preprocess_model                                                                        AS model,
        vin,
        TRIM(details)                                                                           AS model_description,
        CAST(CASE WHEN preprocess_price = '' THEN NULL ELSE preprocess_price END AS NUMERIC)    AS price,
        CAST(preprocess_mileage AS INTEGER)                                                     AS mileage,
        TRIM(preprocess_city)                                                                   AS listing_city,
        TRIM(preprocess_state)                                                                  AS listing_state,
        CAST(TRIM(preprocess_zip) AS INTEGER)                                                   AS listing_zip,
        CAST(year_number || '-' || month_number || '-' || day_number AS DATE)                   AS listing_date,
        listing_id,
        TRIM(_url)                                                                              AS listing_url,
        preprocess_fetch_ts                                                                     AS fetch_ts
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