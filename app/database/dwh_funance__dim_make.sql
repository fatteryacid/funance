CREATE TABLE IF NOT EXISTS dwh_funance.dim_make
(
    make_id        SMALLSERIAL,
    make_name      TEXT,


    PRIMARY KEY (make_id)
)
;

-- Defaults
INSERT INTO dwh_funance.dim_make
VALUES 
    (DEFAULT, 'acura'),
    (DEFAULT, 'bmw'),
    (DEFAULT, 'honda'),
    (DEFAULT, 'lexus'),
    (DEFAULT, 'porsche'),
    (DEFAULT, 'toyota')
;