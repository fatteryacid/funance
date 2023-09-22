-- Think about denormalizing later
/*
CREATE TABLE dimension_car (
    car_id              BIGSERIAL       NOT NULL,
    make                TEXT            NOT NULL,
    model               TEXT            NOT NULL,
    model_year          BIGINT          NOT NULL,
    last_modified_ts    TIMESTAMP       NOT NULL,
    PRIMARY KEY (car_id)
);

CREATE TABLE dimension_location (
    location_id         BIGSERIAL       NOT NULL,
    city                TEXT,
    _state              TEXT,
    last_modified_ts    TIMESTAMP       NOT NULL,
    PRIMARY KEY (location_id)
);

CREATE TABLE fact_listings (
    listing_id          TEXT            PRIMARY KEY,
    car_id              BIGINT          REFERENCES dimension_car,
    location_id         BIGINT          REFERENCES dimension_location,
    price_usd           FLOAT,
    mileage             BIGINT,
    listing_date        DATE,
    fetch_ts            TIMESTAMP,
    --CONSTRAINT fk_car FOREIGN KEY(car_id) REFERENCES dimension_car(car_id)
    --CONSTRAINT fk_location FOREIGN KEY(location_id) REFERENCES dimension_location(location_id)
);
*/

CREATE TABLE production_car_analysis_set (
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