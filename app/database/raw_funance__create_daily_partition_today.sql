CREATE OR REPLACE PROCEDURE raw_funance.create_daily_partition_today()
-- Creates a partition for the current day
-- Using Postgres default of Monday as start of week and Sunday as end of week
-- Will not overwrite existing parititons

LANGUAGE plpgsql
AS $$
    DECLARE partition_name TEXT;

    BEGIN
        RAISE NOTICE 'Trigger function create_partition() is being executed';

        partition_name := 'extract_' || TO_CHAR(CURRENT_DATE, 'YYYY_MM_DD');

        IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = partition_name)
            THEN RAISE NOTICE 'Partition % already exists', partition_name;
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = partition_name)
            THEN 
                RAISE NOTICE 'Creating partition: %', partition_name;
                EXECUTE 'CREATE TABLE ' || partition_name || ' PARTITION OF raw_funance.landing_site FOR VALUES FROM (' || QUOTE_LITERAL(CURRENT_DATE) || ') TO (' || QUOTE_LITERAL((CURRENT_DATE + INTERVAL '1 DAY')::DATE) || ')'
            ;
        END IF;
    END;
$$;