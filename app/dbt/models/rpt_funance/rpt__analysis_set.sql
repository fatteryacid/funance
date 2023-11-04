{{ config(materialized='view') }}

SELECT
    base.entry_id,
    base.vin,
    base.listing_zip,
    base.price,
    base.mileage,
    base.model_year::INT    AS model_year,  -- This is coming into metabase as TEXT but is correctly typed in Postgres
    d_location.state_name,
    d_location.state_abbreviation,
    d_model.model_name,
    d_make.make_name,
    d_color.color_name,
    d_transmission.transmission_type,
    base.listing_url,
    base.fetch_ts
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