SELECT
    base.pk_id      AS id,
    base.vin,
    base.listing_zip,
    base.price,
    base.mileage,
    base.model_year,
    base.listing_url,
    d_location.location_id,
    d_model.model_id,
    d_color.color_id
FROM {{ ref('staging__base_data') }}                        AS base
LEFT JOIN {{ source('dim_tables', 'dim_location')}}         AS d_location
    ON base.listing_state = d_location.state_abbreviation
LEFT JOIN {{ source('dim_tables', 'dim_model')}}            AS d_model
    ON base.model = d_model.model_name
LEFT JOIN {{ source('dim_tables', 'dim_color')}}            AS d_color
    ON base.color = d_color.color_name
LEFT JOIN {{ source('dim_tables', 'dim_transmission')}}     AS d_transmission
    ON base.transmission = d_transmission.transmission_type
