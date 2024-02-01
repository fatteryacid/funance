WITH 
upstream AS (
    SELECT *
    FROM {{ ref('staging__pruned_data') }}
),

supporting_metadata AS (
    SELECT
        *,
        REGEXP_REPLACE(fetch_ts::TEXT, '[-\s:]+', '', 'g')  AS ts_string
    FROM upstream
)

SELECT
    CONCAT(vin, ts_string)      AS pk_id,
    *
FROM supporting_metadata