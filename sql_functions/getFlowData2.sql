CREATE OR REPLACE FUNCTION gwportal.get_flow_data2(
    p_loc_ids TEXT,
    p_type TEXT
)
RETURNS TABLE (
    reading_date TEXT,
    water_elevation NUMERIC
) AS $$

DECLARE
    v_sql TEXT;
BEGIN
    -- Validate input parameters
    IF p_loc_ids IS NULL OR p_loc_ids = '' THEN
        RAISE EXCEPTION 'Invalid well ID provided.';
    END IF;

    IF p_type NOT IN ('daily', 'monthly') THEN
        RAISE EXCEPTION 'Invalid type provided.';
    END IF;

    -- Modified daily aggregation query with strict partitioning
    IF p_type = 'daily' THEN
        v_sql := FORMAT('
            WITH daily_avg AS (
                SELECT 
                    CAST(FLOWDATE as Date) as flow_day,
                    TRUNC(AVG(Discharge), 4) as avg_discharge,
                    ROW_NUMBER() OVER (PARTITION BY CAST(FLOWDATE as Date) ORDER BY CAST(FLOWDATE as Date)) as rn
                FROM gwportal.ugs_gw_manualdata 
                WHERE LOCATIONID IN (%s)
                GROUP BY CAST(FLOWDATE as Date)
            )
            SELECT 
                to_char(flow_day, ''MM/DD/YYYY'') as flow_date,
                avg_discharge as discharge
            FROM daily_avg
            WHERE rn = 1
            ORDER BY flow_day', p_loc_ids);
    ELSE
        v_sql := FORMAT('
            SELECT 
                CAST(EXTRACT(MONTH FROM flowdate) as varchar) || ''/'' || CAST(EXTRACT(YEAR FROM flowdate) AS varchar) as flow_date, 
                TRUNC(AVG(Discharge), 4) as discharge 
            FROM gwportal.ugs_gw_manualdata 
            WHERE LOCATIONID IN (%s) 
            GROUP BY 
                CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || ''-'' || RIGHT(''00'' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2), 
                CAST(EXTRACT(MONTH FROM flowdate) as varchar) || ''/'' || CAST(EXTRACT(YEAR FROM flowdate) AS varchar) 
            ORDER BY 
                CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || ''-'' || RIGHT(''00'' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2)', p_loc_ids);
    END IF;

    RETURN QUERY EXECUTE v_sql;
END;
$$ LANGUAGE plpgsql;

-- DECLARE
--     v_sql TEXT;
-- BEGIN
--     -- Validate input parameters
--     IF p_loc_ids IS NULL OR p_loc_ids = '' THEN
--         RAISE EXCEPTION 'Invalid location ID provided.';
--     END IF;
--     IF p_type NOT IN ('daily', 'monthly') THEN
--         RAISE EXCEPTION 'Invalid type provided.';
--     END IF;

--     -- daily aggregation query
--     IF p_type = 'daily' THEN
--         v_sql := FORMAT('
--             WITH daily_avg AS (
--                 SELECT
--                     CAST(readingdate as Date) as reading_day,
--                     TRUNC(AVG(waterelevation), 4) as avg_elevation
--                 FROM gwportal.reading
--                 WHERE locationid IN (%s)
--                 GROUP BY CAST(readingdate as Date)
--             )
--             SELECT
--                 to_char(reading_day, ''MM/DD/YYYY'') as reading_date,
--                 avg_elevation as water_elevation
--             FROM daily_avg
--             ORDER BY reading_day', p_loc_ids);
--     ELSE
--         v_sql := FORMAT('
--             SELECT
--                 CAST(EXTRACT(MONTH FROM readingdate) as varchar) || ''/'' || CAST(EXTRACT(YEAR FROM readingdate) AS varchar) as reading_date,
--                 TRUNC(AVG(waterelevation), 4) as water_elevation
--             FROM gwportal.reading
--             WHERE locationid IN (%s)
--             GROUP BY
--                 CAST(EXTRACT(YEAR FROM readingdate) AS varchar) || ''-'' || RIGHT(''00'' || CAST(EXTRACT(MONTH FROM readingdate) AS varchar), 2),
--                 CAST(EXTRACT(MONTH FROM readingdate) as varchar) || ''/'' || CAST(EXTRACT(YEAR FROM readingdate) AS varchar)
--             ORDER BY
--                 CAST(EXTRACT(YEAR FROM readingdate) AS varchar) || ''-'' || RIGHT(''00'' || CAST(EXTRACT(MONTH FROM readingdate) AS varchar), 2)', p_loc_ids);
--     END IF;

--     RETURN QUERY EXECUTE v_sql;
-- END;