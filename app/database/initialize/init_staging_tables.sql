CREATE TABLE staging_raw_data (
    empty_set               TEXT,
    year_string             TEXT,
    make                    TEXT,   -- Does not need processing
    model                   TEXT,   -- Does not need processing
    price                   TEXT,
    mileage                 TEXT,
    listing_location        TEXT,
    listing_date            TEXT,
    listing_id              TEXT,   -- Does not need processing
    listing_url             TEXT,
    fetch_ts                TEXT
);

CREATE TABLE staging_processed_data (
    model_year              BIGINT,
    make                    TEXT,
    model                   TEXT,
    model_description       TEXT,
    price                   NUMERIC(9,2),
    mileage                 BIGINT,
    listing_city            TEXT,
    listing_state           TEXT,
    listing_date            DATE,
    listing_id              TEXT,
    listing_url             TEXT,
    fetch_ts                TIMESTAMP
);
