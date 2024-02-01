CREATE TABLE IF NOT EXISTS dwh_funance.dim_color
(
    color_id    SMALLSERIAL,
    color_name  TEXT,


    PRIMARY KEY (color_id)
)
;

-- Defaults
INSERT INTO dwh_funance.dim_color
VALUES
    (DEFAULT, 'white'),
    (DEFAULT, 'black'),
    (DEFAULT, 'gray'),
    (DEFAULT, 'silver'),
    (DEFAULT, 'red'),
    (DEFAULT, 'orange'),
    (DEFAULT, 'yellow'),
    (DEFAULT, 'green'),
    (DEFAULT, 'blue'),
    (DEFAULT, 'purple')
;