CREATE OR REPLACE FUNCTION sde.get_flow_data(
    p_loc_ids TEXT,
    p_type TEXT
)
RETURNS TABLE (
    flow_date TEXT,
    discharge NUMERIC
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

    -- Prepare the SQL query based on the type
    IF p_type = 'daily' THEN
        v_sql := FORMAT('
            SELECT 
                to_char(FLOWDATE, ''MM/DD/YYYY'') as flow_date, 
                TRUNC(Sum(Discharge) / COUNT(CAST(FLOWDATE as Date)), 4) as discharge 
            FROM sde.UGS_GW_FLOW 
            WHERE LOCATIONID IN (%s) 
            GROUP BY to_char(FLOWDATE, ''MM/DD/YYYY''), CAST(FLOWDATE as Date) 
            ORDER BY CAST(FLOWDATE as date)', p_loc_ids);
    ELSE -- monthly
        v_sql := FORMAT('
            SELECT 
                CAST(EXTRACT(MONTH FROM flowdate) as varchar) || ''/'' || CAST(EXTRACT(YEAR FROM flowdate) AS varchar) as flow_date, 
                TRUNC(Sum(Discharge) / COUNT(CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || ''-'' || RIGHT (''00'' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2)), 4) as discharge 
            FROM sde.UGS_GW_FLOW 
            WHERE LOCATIONID IN (%s) 
            GROUP BY 
                CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || ''-'' || RIGHT(''00'' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2), 
                CAST(EXTRACT(MONTH FROM flowdate) AS varchar) || ''/'' || CAST(EXTRACT(YEAR FROM flowdate) AS varchar) 
            ORDER BY 
                CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || ''-'' || RIGHT (''00'' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2)', p_loc_ids);
    END IF;

    -- Execute the dynamic SQL and return the result
    RETURN QUERY EXECUTE v_sql;
END;
$$ LANGUAGE plpgsql;

-- Grant necessary permissions
GRANT EXECUTE ON FUNCTION sde.get_flow_data(TEXT, TEXT) TO web_anon;