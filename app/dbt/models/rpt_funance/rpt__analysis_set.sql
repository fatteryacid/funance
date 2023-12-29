{{ config(materialized='view') }}

SELECT
    base.entry_id,
    base.vin,
    base.listing_zip,
    base.price,
    base.mileage,
    base.model_year::INT                                AS model_year,  -- This is coming into metabase as TEXT but is correctly typed in Postgres
    d_location.state_name,
    d_location.state_abbreviation,
    d_model.model_name,
    d_make.make_name,
    d_color.color_name,
    d_transmission.transmission_type,
    base.listing_url,
    base.fetch_ts,
    COALESCE(base.fetch_ts::DATE = CURRENT_DATE, FALSE) AS is_today,
    price - COALESCE(LAG(price, 1) OVER(PARTITION BY vin ORDER BY fetch_ts ASC), 0)     AS previous_price,
    CASE
        WHEN price < 0 THEN 'Error'
        WHEN price BETWEEN 0 AND 5000 THEN '$0 - $5k'
        WHEN price BETWEEN 5001 AND 10000 THEN '$5k - $10k'
        WHEN price BETWEEN 10001 AND 15000 THEN '$10k - $15k'
        WHEN price BETWEEN 15001 AND 20000 THEN '$15k - $20k'
        WHEN price BETWEEN 20001 AND 25000 THEN '$20k - $25k'
        WHEN price BETWEEN 25001 AND 30000 THEN '$25k - $30k'
        WHEN price BETWEEN 30001 AND 35000 THEN '$30k - $35k'
        WHEN price BETWEEN 35001 AND 40000 THEN '$35k - $40k'
        WHEN price BETWEEN 40001 AND 45000 THEN '$40k - $45k'
        WHEN price BETWEEN 45001 AND 50000 THEN '$45k - $50k'
        WHEN price BETWEEN 50001 AND 55000 THEN '$50k - $55k'
        WHEN price BETWEEN 55001 AND 60000 THEN '$55k - $60k'
        WHEN price BETWEEN 60001 AND 65000 THEN '$60k - $65k'
        WHEN price BETWEEN 65001 AND 70000 THEN '$65k - $70k'
        WHEN price BETWEEN 70001 AND 75000 THEN '$70k - $75k'
        WHEN price BETWEEN 75001 AND 80000 THEN '$75k - $80k'
        WHEN price BETWEEN 80001 AND 85000 THEN '$80k - $85k'
        WHEN price BETWEEN 85001 AND 90000 THEN '$85k - $90k'
        WHEN price BETWEEN 90001 AND 95000 THEN '$90k - $95k'
        WHEN price BETWEEN 95001 AND 100000 THEN '$95k - $100k'
        ELSE '$100k+'
    END                 AS price_band,
    CASE
        WHEN mileage < 0 THEN 'Error'
        WHEN mileage BETWEEN 0 AND 5000 THEN '0 - 5k'
        WHEN mileage BETWEEN 5001 AND 10000 THEN '5k - 10k'
        WHEN mileage BETWEEN 10001 AND 15000 THEN '10k - 15k'
        WHEN mileage BETWEEN 15001 AND 20000 THEN '15k - 20k'
        WHEN mileage BETWEEN 20001 AND 25000 THEN '20k - 25k'
        WHEN mileage BETWEEN 25001 AND 30000 THEN '25k - 30k'
        WHEN mileage BETWEEN 30001 AND 35000 THEN '30k - 35k'
        WHEN mileage BETWEEN 35001 AND 40000 THEN '35k - 40k'
        WHEN mileage BETWEEN 40001 AND 45000 THEN '40k - 45k'
        WHEN mileage BETWEEN 45001 AND 50000 THEN '45k - 50k'
        WHEN mileage BETWEEN 50001 AND 55000 THEN '50k - 55k'
        WHEN mileage BETWEEN 55001 AND 60000 THEN '55k - 60k'
        WHEN mileage BETWEEN 60001 AND 65000 THEN '60k - 65k'
        WHEN mileage BETWEEN 65001 AND 70000 THEN '65k - 70k'
        WHEN mileage BETWEEN 70001 AND 75000 THEN '70k - 75k'
        WHEN mileage BETWEEN 75001 AND 80000 THEN '75k - 80k'
        WHEN mileage BETWEEN 80001 AND 85000 THEN '80k - 85k'
        WHEN mileage BETWEEN 85001 AND 90000 THEN '85k - 90k'
        WHEN mileage BETWEEN 90001 AND 95000 THEN '90k - 95k'
        WHEN mileage BETWEEN 95001 AND 100000 THEN '95k - 100k'
        ELSE '100k+'
    END                 AS mileage_band,
    CASE
        WHEN model_name = 'm3' AND model_year BETWEEN 1986 AND 1991 THEN 'E30 M3'
        WHEN model_name = 'm3' AND model_year BETWEEN 1992 AND 1999 THEN 'E36 M3'
        WHEN model_name = 'm3' AND model_year BETWEEN 2000 AND 2006 THEN 'E46 M3'
        WHEN model_name = 'm3' AND model_year BETWEEN 2007 AND 2013 THEN 'E9x M3'
        WHEN model_name = 'm3' AND model_year BETWEEN 2014 AND 2019 THEN 'F80 M3'
        WHEN model_name = 'm3' AND model_year BETWEEN 2020 AND EXTRACT(YEAR FROM CURRENT_DATE) THEN 'G80 M3'
        ELSE model_name
    END                 AS enthusiast_model_alias
FROM {{ ref('dwh__fact_set') }}                             AS base
LEFT JOIN {{ source('dim_tables', 'dim_location')}}         AS d_location
    ON base.location_id = d_location.location_id
LEFT JOIN {{ source('dim_tables', 'dim_model')}}            AS d_model
    ON base.model_id = d_model.model_id
LEFT JOIN {{ source('dim_tables', 'dim_make') }}            AS d_make
    ON d_model.make_id = d_make.make_id
LEFT JOIN {{ source('dim_tables', 'dim_color')}}            AS d_color
    ON base.color_id = d_color.color_id
LEFT JOIN {{ source('dim_tables', 'dim_transmission')}}     AS d_transmission
    ON base.transmission_id = d_transmission.transmission_id
WHERE 1=1