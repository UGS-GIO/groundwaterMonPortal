CREATE OR REPLACE FUNCTION sde.get_well_reading_data(
    p_loc_id TEXT,
    p_type TEXT,
    p_from_date DATE,
    p_to_date DATE
)
RETURNS TEXT AS $$
DECLARE
    v_sql TEXT;
    v_result TEXT;
BEGIN
    IF p_type = 'all' THEN
        v_sql := 'SELECT 
            CAST(readingdate as varchar(19)) as "Reading Date",
            MEASUREDLEVEL as "Measured Level",
            Temperature as Temp,
            BAROEFFICIENCYLEVEL as "Baro Efficiency Level",
            MEASUREDDTW as "Measured DTW",
            DRIFTCORRECTION as "Drift Correction",
            WATERELEVATION as "Water Elevation"
        FROM sde.reading 
        WHERE LOCATIONID = ''' || p_loc_id || '''
        AND READINGDATE BETWEEN ''' || p_from_date || ''' AND ''' || p_to_date || '''
        ORDER BY READINGDATE';
    ELSIF p_type = 'daily' THEN
        v_sql := 'SELECT 
            TO_CHAR(READINGDATE, ''DD/MM/YYYY'') as "Reading Date",
            ROUND(AVG(MEASUREDLEVEL), 4) as "Measured Level",
            ROUND(AVG(TEMPERATURE), 4) as TEMPERATURE,
            ROUND(AVG(BAROEFFICIENCYLEVEL), 4) as "Barometric Efficiency Level",
            ROUND(AVG(MEASUREDDTW), 4) as "Measured DTW",
            ROUND(AVG(DRIFTCORRECTION), 4) as "Drift Correction",
            ROUND(AVG(WATERELEVATION), 4) as "Water Elevation"
        FROM sde.reading 
        WHERE LOCATIONID = ''' || p_loc_id || '''
        AND READINGDATE BETWEEN ''' || p_from_date || ''' AND ''' || p_to_date || '''
        GROUP BY TO_CHAR(READINGDATE, ''DD/MM/YYYY''), CAST(READINGDATE as Date)
        ORDER BY CAST(READINGDATE as date)';
    ELSIF p_type = 'monthly' THEN
        v_sql := 'SELECT 
            CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar) || ''/'' || CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) as "Reading Date",
            ROUND(AVG(MEASUREDLEVEL), 3) as "Measured Level",
            ROUND(AVG(Temperature), 3) as Temp,
            ROUND(AVG(BAROEFFICIENCYLEVEL), 3) as "Barometric Efficiency Level",
            ROUND(AVG(MEASUREDDTW), 3) as "Measured DTW",
            ROUND(AVG(DRIFTCORRECTION), 5) as "Drift Correction",
            ROUND(AVG(WATERELEVATION), 3) as "Water Elevation"
        FROM sde.reading 
        WHERE LOCATIONID = ''' || p_loc_id || '''
        AND READINGDATE BETWEEN ''' || p_from_date || ''' AND ''' || p_to_date || '''
        GROUP BY EXTRACT(YEAR FROM READINGDATE), EXTRACT(MONTH FROM READINGDATE)
        ORDER BY EXTRACT(YEAR FROM READINGDATE), EXTRACT(MONTH FROM READINGDATE)';
    END IF;

    EXECUTE 'SELECT string_agg(t::text, E''\n'')
             FROM (' || v_sql || ') t' INTO v_result;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql;