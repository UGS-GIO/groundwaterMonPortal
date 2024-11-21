CREATE OR REPLACE FUNCTION gwportal.get_elevation_data(
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
        RAISE EXCEPTION 'Invalid location ID provided.';
    END IF;
    IF p_type NOT IN ('daily', 'monthly') THEN
        RAISE EXCEPTION 'Invalid type provided.';
    END IF;

    -- daily aggregation query
    IF p_type = 'daily' THEN
        v_sql := FORMAT('
            WITH daily_avg AS (
                SELECT
                    CAST(readingdate as Date) as reading_day,
                    TRUNC(AVG(waterelevation), 4) as avg_elevation
                FROM gwportal.reading
                WHERE locationid IN (%s)
                GROUP BY CAST(readingdate as Date)
            )
            SELECT
                to_char(reading_day, ''MM/DD/YYYY'') as reading_date,
                avg_elevation as water_elevation
            FROM daily_avg
            ORDER BY reading_day', p_loc_ids);
    ELSE
        v_sql := FORMAT('
            SELECT
                CAST(EXTRACT(MONTH FROM readingdate) as varchar) || ''/'' || CAST(EXTRACT(YEAR FROM readingdate) AS varchar) as reading_date,
                TRUNC(AVG(waterelevation), 4) as water_elevation
            FROM gwportal.reading
            WHERE locationid IN (%s)
            GROUP BY
                CAST(EXTRACT(YEAR FROM readingdate) AS varchar) || ''-'' || RIGHT(''00'' || CAST(EXTRACT(MONTH FROM readingdate) AS varchar), 2),
                CAST(EXTRACT(MONTH FROM readingdate) as varchar) || ''/'' || CAST(EXTRACT(YEAR FROM readingdate) AS varchar)
            ORDER BY
                CAST(EXTRACT(YEAR FROM readingdate) AS varchar) || ''-'' || RIGHT(''00'' || CAST(EXTRACT(MONTH FROM readingdate) AS varchar), 2)', p_loc_ids);
    END IF;

    RETURN QUERY EXECUTE v_sql;
END;
$$ LANGUAGE plpgsql;