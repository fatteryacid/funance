SELECT
    vehicle_vin || REGEXP_REPLACE(job_dt::TEXT, '-', '', 'g')   AS id,
    job_ts,
    job_dt,
    location_address,
    location_zipcode,
    location_country,
    vehicle_trim,
    CASE 
        WHEN vehicle_mileage = '' THEN NULL
        WHEN REGEXP_REPLACE(vehicle_mileage, '[^0-9]+', '', 'g') = '' THEN NULL
        ELSE REGEXP_REPLACE(vehicle_mileage, '[^0-9]+', '', 'g')::INTEGER
    END                 AS vehicle_mileage,
    CASE
        WHEN backend_model ILIKE '%acura%' THEN 'Acura'
        WHEN backend_model ILIKE '%bmw%' THEN 'BMW'
        WHEN backend_model ILIKE '%lexus%' THEN 'Lexus'
        WHEN backend_model ILIKE '%porsche%' THEN 'Porsche'
        WHEN backend_model ILIKE '%honda%' THEN 'Honda'
        WHEN backend_model ILIKE '%toyota%' THEN 'Toyota'
        WHEN backend_model ILIKE '%subaru%' THEN 'Subaru'
        WHEN backend_model ILIKE '%mazda%' THEN 'Mazda'
        ELSE INITCAP(SPLIT_PART(backend_model, '_', 1))
    END                 AS vehicle_make,
    CASE
        WHEN backend_model ILIKE '%integra_type_s%' THEN 'Integra Type S'
        WHEN backend_model ILIKE '%m3%'             THEN 'M3'
        WHEN backend_model ILIKE '%gs_f%'           THEN 'GS F'
        WHEN backend_model ILIKE '%rc_f%'           THEN 'RC F'
        WHEN backend_model ILIKE '%is_500%'         THEN 'IS 500'
        WHEN backend_model ILIKE '%civic_type_r%'   THEN 'Civic Type R'
        WHEN backend_model ILIKE '%911%'            THEN 'Carrera 911'
        WHEN backend_model ILIKE '%sti%'            THEN 'WRX STI'
        WHEN backend_model ILIKE '%gr_supra%'       THEN 'GR Supra'
        WHEN backend_model ILIKE '%mx5%'            THEN 'MX-5 Miata'
        ELSE backend_model
    END                 AS vehicle_model,
    vehicle_year,
    CASE
        WHEN vehicle_price = '' THEN NULL
        WHEN REGEXP_REPLACE(vehicle_price, '[^0-9]+', '', 'g') = '' THEN NULL
        ELSE REGEXP_REPLACE(vehicle_price, '[^0-9]+', '', 'g')::NUMERIC
    END                 AS vehicle_price,
    vehicle_vin,
    vehicle_image_url,
    listing_url,
    listing_details,
    CASE 
        WHEN listing_date ILIKE '%days ago%' OR listing_date IN ('Today', 'Yesterday') THEN
            CASE
                WHEN listing_date ILIKE '%days ago%' THEN job_dt - CAST(SPLIT_PART(listing_date, ' ', 1) || ' ' || 'DAY' AS INTERVAL) 
                WHEN listing_date = 'Yesterday' THEN job_dt - CAST('1 DAY' AS INTERVAL)
                WHEN listing_date = 'Today' THEN job_dt
                ELSE NULL
            END
        ELSE
            -- Previously ran into issue where listing dates from the previous year were not identified. This solves that issue
            -- However, this is only scalable for a 1-2 year relationship
            CASE 
                WHEN TO_DATE(TRIM(REGEXP_REPLACE(listing_date, '(st|th|rd|nd)', '', 'g')) || ' ' || EXTRACT(YEAR FROM job_dt), 'Mon DD YYYY')::DATE > job_dt THEN (TO_DATE(TRIM(REGEXP_REPLACE(listing_date, '(st|th|rd|nd)', '', 'g')) || ' ' || EXTRACT(YEAR FROM job_dt), 'Mon DD YYYY')::DATE - INTERVAL '1 YEAR')
                ELSE TO_DATE(TRIM(REGEXP_REPLACE(listing_date, '(st|th|rd|nd)', '', 'g')) || ' ' || EXTRACT(YEAR FROM job_dt), 'Mon DD YYYY')::DATE
            END
    END                 AS listing_date
FROM {{ ref('staging__unpacked_data') }}
