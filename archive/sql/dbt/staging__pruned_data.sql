WITH
source AS (
    SELECT *
    FROM {{ source('staging_funance', 'raw_data') }}
),

data_cleaning_layer AS (
    SELECT 
        *,
        LOWER(make)                                                         AS preprocess_make,
        CASE
            WHEN model = 'rcf' THEN 'rc_f'   -- This is a temp fix, will need to standardize these model name representation in the scraper
            WHEN model = 'gsf' THEN 'gs_f'
            WHEN model = 'is500' THEN 'is_500'
            WHEN model = 'civictyper' THEN 'civic_type_r'
            ELSE model
        END                                                                 AS preprocess_model,
        SPLIT_PART(_location, ',', 1)                                       AS preprocess_city,
        SPLIT_PART(_location, ',', 2)                                       AS preprocess_state,
        TRIM(CASE
            WHEN REGEXP_LIKE(location_zipcode, '[^0-9]+', 'i') THEN NULL
            ELSE location_zipcode
        END)                                                                AS preprocess_zip,
        REGEXP_REPLACE(price, '[^0-9]+', '', 'g')                           AS preprocess_price,
        CASE 
            WHEN REGEXP_REPLACE(mileage, '[^0-9]+', '', 'g') = '' THEN NULL
            ELSE REGEXP_REPLACE(mileage, '[^0-9]+', '', 'g')
        END                                                                 AS preprocess_mileage,
        CASE 
            WHEN listing_date LIKE '%days ago%' OR listing_date IN ('Today', 'Yesterday') THEN TRUE 
            ELSE FALSE 
        END                                                                 AS requires_arithmetic,
        CAST(fetch_ts AS TIMESTAMP)                                         AS preprocess_fetch_ts
    FROM source
    WHERE 1=1
        AND CAST(fetch_ts AS DATE) = CURRENT_DATE   -- Limit copy to only current date
),

data_casting_layer AS (
    SELECT
        CAST(_year AS INTEGER)                                                                      AS model_year,
        preprocess_make                                                                             AS make,
        preprocess_model                                                                            AS model,
        vin,
        TRIM(details)                                                                               AS model_description,
        CAST(CASE WHEN preprocess_price = '' THEN NULL ELSE preprocess_price END AS NUMERIC)        AS price,
        CAST(CASE WHEN preprocess_mileage = '' THEN NULL ELSE preprocess_mileage END AS INTEGER)    AS mileage,
        TRIM(preprocess_city)                                                                       AS listing_city,
        TRIM(preprocess_state)                                                                      AS listing_state,
        CAST(CASE WHEN preprocess_zip = '' THEN NULL ELSE preprocess_zip END AS INTEGER)            AS listing_zip,
        listing_date,
        listing_id,
        TRIM(_url)                                                                                  AS listing_url,
        preprocess_fetch_ts                                                                         AS fetch_ts,
        requires_arithmetic
    FROM data_cleaning_layer
),

date_calculation_metadata AS (
    -- Parse out integer to perform date arithmetic
    SELECT 
        *,
        CASE WHEN requires_arithmetic THEN 
            CASE 
                WHEN listing_date LIKE '%days ago%' THEN CAST(SPLIT_PART(listing_date, ' ', 1) AS INTEGER)
                WHEN listing_date = 'Yesterday' THEN 1
                WHEN listing_date = 'Today' THEN 0
                ELSE NULL
            END                                                                 
        ELSE NULL END                                                                                                               AS number_days,

        CASE WHEN NOT requires_arithmetic THEN
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
            END                                                                     
        ELSE NULL END                                                                                                               AS month_number,
        CASE WHEN NOT requires_arithmetic THEN REGEXP_REPLACE(SPLIT_PART(listing_date, ' ', 2), '[^0-9]+', '', 'g') ELSE NULL END   AS day_number,
        CASE WHEN NOT requires_arithmetic THEN EXTRACT(YEAR FROM fetch_ts)   ELSE NULL END                                          AS year_number

    FROM data_casting_layer
    WHERE 1=1
        --AND requires_arithmetic
),

pruned_set AS (
    -- Final set for arithmetic-supportable data points
    SELECT
        model_year::INT,
        make,
        model,
        vin,
        model_description,
        price,
        mileage,
        listing_city,
        listing_state,
        listing_zip,
        NULL        AS color,           -- Placeholder
        NULL        AS transmission,
        CASE 
            WHEN requires_arithmetic THEN CAST(fetch_ts - CAST(number_days || ' ' || 'DAY' AS INTERVAL) AS DATE)  
            WHEN NOT requires_arithmetic THEN CAST(year_number || '-' || month_number || '-' || day_number AS DATE)
            ELSE NULL
        END                 AS listing_date,
        listing_id,
        listing_url,
        fetch_ts
    FROM date_calculation_metadata
)

SELECT * 
FROM pruned_set
