CREATE TABLE IF NOT EXISTS dwh_funance.dim_transmission
(
    transmission_id     SMALLSERIAL,
    transmission_type   TEXT,

    
    PRIMARY KEY (transmission_id)
)
;

-- Defaults
INSERT INTO dwh_funance.dim_transmission
VALUES 
    (DEFAULT, 'automatic'),
    (DEFAULT, 'manual')
;