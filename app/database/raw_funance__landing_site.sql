--DROP SCHEMA raw_funance;
--CREATE SCHEMA IF NOT EXISTS raw_funance;
--SET search_path TO raw_funance;
--DROP TABLE raw_funance.landing_site;

CREATE TABLE IF NOT EXISTS raw_funance.landing_site
(
    backend_model   TEXT,
    raw_data        JSONB,
    job_ts          TIMESTAMP,
    job_dt          DATE
)
PARTITION BY RANGE (job_dt)
;

/*
INSERT INTO raw_funance.landing_site
VALUES
(
    'test',
    '{"test": "test"},
    NULL,
    '2024-01-24'
);
*/