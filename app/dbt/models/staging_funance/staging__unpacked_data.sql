SELECT  
    landing.backend_model,
    landing.job_ts,
    landing.job_dt,
    elem->>'location'       AS location_address,
    elem->>'locationCode'   AS location_zipcode,
    elem->>'countryCode'    AS location_country,
    elem->>'trim'           AS vehicle_trim,
    elem->>'mileage'        AS vehicle_mileage,
    elem->>'year'           AS vehicle_year,
    elem->>'price'          AS vehicle_price,
    elem->>'vin'            AS vehicle_vin,
    elem->>'img'            AS vehicle_image_url,
    elem->>'url'            AS listing_url,
    elem->>'details'        AS listing_details,
    elem->>'date'           AS listing_date
FROM {{ source('raw_funance', 'landing_site') }}        AS landing,
LATERAL jsonb_array_elements(raw_data->'results')       AS elem
WHERE 1=1