{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}


SELECT
    *
FROM {{ ref('staging__typecasted_data') }}
WHERE 1=1

{% if is_incremental() %}

    AND job_dt > (SELECT MAX(job_dt) FROM {{ this }})

{% endif %}
