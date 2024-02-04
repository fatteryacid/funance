{{ config(materialized='view') }}

SELECT 
    *,
    CASE
        WHEN listing_details ILIKE '%automatic transmission%' THEN FALSE
        WHEN listing_details ILIKE '%manual transmission%' THEN TRUE
        ELSE NULL
    END                             AS vehicle_is_manual,
    CASE
        WHEN vehicle_price < 0 OR vehicle_price IS NULL THEN 'Error'
        WHEN vehicle_price BETWEEN 0 AND 5000           THEN '$0 - $5k'
        WHEN vehicle_price BETWEEN 5001 AND 10000       THEN '$5k - $10k'
        WHEN vehicle_price BETWEEN 10001 AND 15000      THEN '$10k - $15k'
        WHEN vehicle_price BETWEEN 15001 AND 20000      THEN '$15k - $20k'
        WHEN vehicle_price BETWEEN 20001 AND 25000      THEN '$20k - $25k'
        WHEN vehicle_price BETWEEN 25001 AND 30000      THEN '$25k - $30k'
        WHEN vehicle_price BETWEEN 30001 AND 35000      THEN '$30k - $35k'
        WHEN vehicle_price BETWEEN 35001 AND 40000      THEN '$35k - $40k'
        WHEN vehicle_price BETWEEN 40001 AND 45000      THEN '$40k - $45k'
        WHEN vehicle_price BETWEEN 45001 AND 50000      THEN '$45k - $50k'
        WHEN vehicle_price BETWEEN 50001 AND 55000      THEN '$50k - $55k'
        WHEN vehicle_price BETWEEN 55001 AND 60000      THEN '$55k - $60k'
        WHEN vehicle_price BETWEEN 60001 AND 65000      THEN '$60k - $65k'
        WHEN vehicle_price BETWEEN 65001 AND 70000      THEN '$65k - $70k'
        WHEN vehicle_price BETWEEN 70001 AND 75000      THEN '$70k - $75k'
        WHEN vehicle_price BETWEEN 75001 AND 80000      THEN '$75k - $80k'
        WHEN vehicle_price BETWEEN 80001 AND 85000      THEN '$80k - $85k'
        WHEN vehicle_price BETWEEN 85001 AND 90000      THEN '$85k - $90k'
        WHEN vehicle_price BETWEEN 90001 AND 95000      THEN '$90k - $95k'
        WHEN vehicle_price BETWEEN 95001 AND 100000     THEN '$95k - $100k'
        ELSE '$100k+'
    END                             AS vehicle_price_band,
    -- Stop gap hardcode solution until scalable solution is found
    CASE
        WHEN vehicle_price < 0 OR vehicle_price IS NULL THEN NULL
        WHEN vehicle_price BETWEEN 0 AND 5000           THEN 1
        WHEN vehicle_price BETWEEN 5001 AND 10000       THEN 2
        WHEN vehicle_price BETWEEN 10001 AND 15000      THEN 3
        WHEN vehicle_price BETWEEN 15001 AND 20000      THEN 4
        WHEN vehicle_price BETWEEN 20001 AND 25000      THEN 5
        WHEN vehicle_price BETWEEN 25001 AND 30000      THEN 6
        WHEN vehicle_price BETWEEN 30001 AND 35000      THEN 7
        WHEN vehicle_price BETWEEN 35001 AND 40000      THEN 8
        WHEN vehicle_price BETWEEN 40001 AND 45000      THEN 9
        WHEN vehicle_price BETWEEN 45001 AND 50000      THEN 10
        WHEN vehicle_price BETWEEN 50001 AND 55000      THEN 11
        WHEN vehicle_price BETWEEN 55001 AND 60000      THEN 12
        WHEN vehicle_price BETWEEN 60001 AND 65000      THEN 13
        WHEN vehicle_price BETWEEN 65001 AND 70000      THEN 14
        WHEN vehicle_price BETWEEN 70001 AND 75000      THEN 15
        WHEN vehicle_price BETWEEN 75001 AND 80000      THEN 16
        WHEN vehicle_price BETWEEN 80001 AND 85000      THEN 17
        WHEN vehicle_price BETWEEN 85001 AND 90000      THEN 18
        WHEN vehicle_price BETWEEN 90001 AND 95000      THEN 19
        WHEN vehicle_price BETWEEN 95001 AND 100000     THEN 20
        ELSE 21
    END                             AS vehicle_price_band_rank,
    CASE
        WHEN vehicle_mileage < 0 OR vehicle_mileage IS NULL THEN 'Error'
        WHEN vehicle_mileage BETWEEN 0 AND 5000             THEN '0 - 5k'
        WHEN vehicle_mileage BETWEEN 5001 AND 10000         THEN '5k - 10k'
        WHEN vehicle_mileage BETWEEN 10001 AND 15000        THEN '10k - 15k'
        WHEN vehicle_mileage BETWEEN 15001 AND 20000        THEN '15k - 20k'
        WHEN vehicle_mileage BETWEEN 20001 AND 25000        THEN '20k - 25k'
        WHEN vehicle_mileage BETWEEN 25001 AND 30000        THEN '25k - 30k'
        WHEN vehicle_mileage BETWEEN 30001 AND 35000        THEN '30k - 35k'
        WHEN vehicle_mileage BETWEEN 35001 AND 40000        THEN '35k - 40k'
        WHEN vehicle_mileage BETWEEN 40001 AND 45000        THEN '40k - 45k'
        WHEN vehicle_mileage BETWEEN 45001 AND 50000        THEN '45k - 50k'
        WHEN vehicle_mileage BETWEEN 50001 AND 55000        THEN '50k - 55k'
        WHEN vehicle_mileage BETWEEN 55001 AND 60000        THEN '55k - 60k'
        WHEN vehicle_mileage BETWEEN 60001 AND 65000        THEN '60k - 65k'
        WHEN vehicle_mileage BETWEEN 65001 AND 70000        THEN '65k - 70k'
        WHEN vehicle_mileage BETWEEN 70001 AND 75000        THEN '70k - 75k'
        WHEN vehicle_mileage BETWEEN 75001 AND 80000        THEN '75k - 80k'
        WHEN vehicle_mileage BETWEEN 80001 AND 85000        THEN '80k - 85k'
        WHEN vehicle_mileage BETWEEN 85001 AND 90000        THEN '85k - 90k'
        WHEN vehicle_mileage BETWEEN 90001 AND 95000        THEN '90k - 95k'
        WHEN vehicle_mileage BETWEEN 95001 AND 100000       THEN '95k - 100k'
        ELSE '100k+'
    END                             AS vehicle_mileage_band,
    -- Stop gap hardcode solution until scalable solution is found
    CASE
        WHEN vehicle_mileage < 0 OR vehicle_mileage IS NULL THEN NULL
        WHEN vehicle_mileage BETWEEN 0 AND 5000             THEN 1
        WHEN vehicle_mileage BETWEEN 5001 AND 10000         THEN 2
        WHEN vehicle_mileage BETWEEN 10001 AND 15000        THEN 3
        WHEN vehicle_mileage BETWEEN 15001 AND 20000        THEN 4
        WHEN vehicle_mileage BETWEEN 20001 AND 25000        THEN 5
        WHEN vehicle_mileage BETWEEN 25001 AND 30000        THEN 6
        WHEN vehicle_mileage BETWEEN 30001 AND 35000        THEN 7
        WHEN vehicle_mileage BETWEEN 35001 AND 40000        THEN 8
        WHEN vehicle_mileage BETWEEN 40001 AND 45000        THEN 9
        WHEN vehicle_mileage BETWEEN 45001 AND 50000        THEN 0
        WHEN vehicle_mileage BETWEEN 50001 AND 55000        THEN 10
        WHEN vehicle_mileage BETWEEN 55001 AND 60000        THEN 11
        WHEN vehicle_mileage BETWEEN 60001 AND 65000        THEN 12
        WHEN vehicle_mileage BETWEEN 65001 AND 70000        THEN 13
        WHEN vehicle_mileage BETWEEN 70001 AND 75000        THEN 14
        WHEN vehicle_mileage BETWEEN 75001 AND 80000        THEN 15
        WHEN vehicle_mileage BETWEEN 80001 AND 85000        THEN 16
        WHEN vehicle_mileage BETWEEN 85001 AND 90000        THEN 17
        WHEN vehicle_mileage BETWEEN 90001 AND 95000        THEN 18
        WHEN vehicle_mileage BETWEEN 95001 AND 100000       THEN 19
        ELSE 20
    END                             AS vehicle_mileage_band_rank,
    EXTRACT(DAY FROM job_dt - listing_date)   AS listing_age,
    CASE 
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 0 AND 7    THEN '0 - 7 days old'
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 8 AND 30   THEN '8 - 30 days old'
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 31 AND 60  THEN '31 to 60 days old'
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 61 AND 90  THEN '61 to 90 days old'
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 91 AND 120 THEN '91 to 120 days old'
        WHEN EXTRACT(DAY FROM job_dt - listing_date) >= 121             THEN '120+ days old'
        ELSE NULL
    END                                             AS listing_age_band,
-- Stop gap hardcode solution until scalable solution is found
    CASE 
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 0 AND 7    THEN 1
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 8 AND 30   THEN 2
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 31 AND 60  THEN 3
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 61 AND 90  THEN 4
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 91 AND 120 THEN 5
        WHEN EXTRACT(DAY FROM job_dt - listing_date) >= 121             THEN 6
        ELSE NULL
    END                                             AS listing_age_band_rank,
    CASE 
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 0 AND 30   THEN 'Suboptimal'
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 31 AND 60  THEN 'Decent'
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 61 AND 120 THEN 'Optimal'
        WHEN EXTRACT(DAY FROM job_dt - listing_date) >= 121             THEN 'Possible demo car'
        ELSE NULL
    END                                             AS negotiation_opportunity_status,
    -- Stop gap hardcode solution until scalable solution is found
    CASE 
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 0 AND 30   THEN 1
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 31 AND 60  THEN 2
        WHEN EXTRACT(DAY FROM job_dt - listing_date) BETWEEN 61 AND 120 THEN 3
        WHEN EXTRACT(DAY FROM job_dt - listing_date) >= 121             THEN 4
        ELSE NULL
    END                                             AS negotiation_opportunity_status_rank

    
FROM {{ ref('dwh__fact_set') }}
