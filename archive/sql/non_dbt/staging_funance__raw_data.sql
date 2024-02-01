-- This needs to be created before running main.py
-- Is the destination for scraper
CREATE TABLE staging_funance.raw_data
(
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
)
;