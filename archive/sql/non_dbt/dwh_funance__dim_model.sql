CREATE TABLE IF NOT EXISTS dwh_funance.dim_model
(
    model_id        SMALLSERIAL,
    make_id         INT,
    model_name      TEXT,


    PRIMARY KEY (model_id),
    FOREIGN KEY (make_id) REFERENCES dwh_funance.dim_make(make_id)
)
;

-- Defaults
INSERT INTO dwh_funance.dim_model
VALUES 
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'acura'), 'integra_type_s'),
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'bmw'), 'm3'),
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'honda'), 'civic_type_r'),
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'lexus'), 'gs_f'),
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'lexus'), 'is_500'),
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'lexus'), 'rc_f'),
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'porsche'), '911'),
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'porsche'), 'cayman'),
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'toyota'), '4runner'),
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'toyota'), 'supra'),
    (DEFAULT, (SELECT make_id FROM dwh_funance.dim_make WHERE make_name = 'toyota'), 'tacoma')
;