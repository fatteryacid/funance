CREATE TABLE IF NOT EXISTS raw_funance.landing_site
(
    backend_model   TEXT,
    raw_data        JSONB,
    job_ts          TIMESTAMP,
    job_dt          DATE
)
PARTITION BY RANGE (job_dt)
;