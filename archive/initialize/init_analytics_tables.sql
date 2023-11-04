-- Think about denormalizing later
CREATE TABLE production_car_analysis_set (
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