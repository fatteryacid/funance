CREATE TABLE staging_raw_data (
    make                TEXT,
    model               TEXT,
    _year               TEXT,
    vin                 TEXT,
    location_zipcode    TEXT,
    _location           TEXT,
    mileage             TEXT,
    price               TEXT,
    _url                TEXT,
    listing_id          TEXT,
    listing_date        TEXT,
    details             TEXT,
    fetch_ts            TEXT
);

CREATE TABLE staging_processed_data (
    model_year              BIGINT,
    make                    TEXT,
    model                   TEXT,
    vin                     TEXT,
    model_description       TEXT,
    price                   NUMERIC(9,2),
    mileage                 BIGINT,
    listing_city            TEXT,
    listing_state           TEXT,
    listing_zip             BIGINT,
    listing_date            DATE,
    listing_id              TEXT,
    listing_url             TEXT,
    fetch_ts                TIMESTAMP
);
