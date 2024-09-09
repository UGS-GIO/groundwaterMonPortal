CREATE OR REPLACE FUNCTION sde.get_spring_data(
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
            TO_CHAR(flowdate,''YYYY-MM-DD HH24:MI:SS'') as "Flow Date",
            Discharge,
            c.Comment
        FROM sde.UGS_GW_FLOW f
        LEFT JOIN sde.UGS_GW_COMMENTS c ON c.Comment_ID = f.COMMENTS
        WHERE LOCATIONID = ''' || p_loc_id || '''
        AND FLOWDATE BETWEEN ''' || p_from_date || ''' AND ''' || p_to_date || '''
        ORDER BY CAST(FLOWDATE as date)';
    ELSIF p_type = 'daily' THEN
        v_sql := 'SELECT 
            TO_CHAR(FLOWDATE, ''DD/MM/YYYY'') as "Flow Date",
            ROUND(AVG(Discharge), 4) as Discharge,
            string_agg(DISTINCT c.Comment, ''; '') as Comment
        FROM sde.UGS_GW_FLOW f
        LEFT JOIN sde.UGS_GW_COMMENTS c ON c.Comment_ID = f.COMMENTS
        WHERE LOCATIONID = ''' || p_loc_id || '''
        AND FLOWDATE BETWEEN ''' || p_from_date || ''' AND ''' || p_to_date || '''
        GROUP BY TO_CHAR(FLOWDATE, ''DD/MM/YYYY''), CAST(FLOWDATE as Date)
        ORDER BY CAST(FLOWDATE as date)';
    ELSIF p_type = 'monthly' THEN
        v_sql := 'SELECT 
            CAST(EXTRACT(MONTH FROM FLOWDATE) AS varchar) || ''/'' || CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar) as "Flow Date",
            ROUND(AVG(Discharge), 4) as Discharge,
            string_agg(DISTINCT c.comment, ''; '') as Comment
        FROM sde.UGS_GW_FLOW f
        LEFT JOIN sde.UGS_GW_COMMENTS c ON c.comment_id = f.comments
        WHERE LOCATIONID = ''' || p_loc_id || '''
        AND FLOWDATE BETWEEN ''' || p_from_date || ''' AND ''' || p_to_date || '''
        GROUP BY EXTRACT(YEAR FROM FLOWDATE), EXTRACT(MONTH FROM FLOWDATE)
        ORDER BY EXTRACT(YEAR FROM FLOWDATE), EXTRACT(MONTH FROM FLOWDATE)';
    END IF;

    EXECUTE 'SELECT string_agg(t::text, E''\n'')
             FROM (' || v_sql || ') t' INTO v_result;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql;