{{
    config(
        materialized='incremental',
        unique_key='entry_id'
    )
}}

SELECT
    base.pk_id                      AS entry_id,
    base.vin,
    base.listing_zip,
    base.price,
    base.mileage,
    base.model_year,
    base.listing_url,
    d_location.location_id,
    d_model.model_id,
    d_color.color_id,
    d_transmission.transmission_id,
    base.fetch_ts
FROM {{ ref('staging__base_data') }}                        AS base
LEFT JOIN {{ source('dim_tables', 'dim_location')}}         AS d_location
    ON base.listing_state = d_location.state_abbreviation
LEFT JOIN {{ source('dim_tables', 'dim_model')}}            AS d_model
    ON base.model = d_model.model_name
LEFT JOIN {{ source('dim_tables', 'dim_color')}}            AS d_color
    ON base.color = d_color.color_name
LEFT JOIN {{ source('dim_tables', 'dim_transmission')}}     AS d_transmission
    ON base.transmission = d_transmission.transmission_type
WHERE 1=1
    AND fetch_ts > (SELECT MAX(fetch_ts) FROM {{ this }})