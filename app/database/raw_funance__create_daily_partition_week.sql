CREATE OR REPLACE PROCEDURE raw_funance.create_daily_partition_week()
-- Creates a partition for every day from the beginning of the week to the end of the week
-- Using Postgres default of Monday as start of week and Sunday as end of week
-- Will not overwrite existing parititons

LANGUAGE plpgsql
AS $$
    DECLARE partition_name TEXT;
    DECLARE dt DATE;
    DECLARE safeguard INTEGER;

    BEGIN
        RAISE NOTICE 'Trigger function create_partition() is being executed';

        dt := DATE_TRUNC('week', CURRENT_DATE)::DATE;

        LOOP 
            partition_name := 'extract_' || TO_CHAR(dt, 'YYYY_MM_DD');
            IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = partition_name)
                THEN RAISE NOTICE 'Partition % already exists', partition_name;
            END IF;

            IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = partition_name)
                THEN 
                    RAISE NOTICE 'Creating partition: %', partition_name;
                    EXECUTE 'CREATE TABLE ' || partition_name || ' PARTITION OF raw_funance.landing_site FOR VALUES FROM (' || QUOTE_LITERAL(dt) || ') TO (' || QUOTE_LITERAL((dt + INTERVAL '1 DAY')::DATE) || ')'
                ;
            END IF;

            dt := DATE_ADD(dt, INTERVAL '1 day')::DATE;
            safeguard := safeguard + 1;

            EXIT WHEN dt > (((DATE_TRUNC('week', CURRENT_DATE)::DATE + INTERVAL '1 WEEK')::DATE) - INTERVAL '1 DAY')::DATE OR safeguard > 14;
        END LOOP;
    END;
$$;

