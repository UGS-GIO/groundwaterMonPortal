CREATE OR REPLACE FUNCTION gwportal.get_groundwater_data(
    p_well_id TEXT,
    p_type TEXT,
    p_from_date DATE,
    p_to_date DATE
) RETURNS TABLE (
    metadata_key TEXT,
    metadata_value TEXT,
    reading_date TEXT,
    measured_level TEXT,
    temperature TEXT,
    baro_efficiency_level TEXT,
    measured_dtw TEXT,
    drift_correction TEXT,
    water_elevation TEXT,
    discharge TEXT,
    comment TEXT
) AS $$
DECLARE
    v_sql TEXT;
    v_location_type TEXT;
    v_metadata RECORD;
BEGIN
    -- Input validation
    IF p_from_date > p_to_date THEN
        RAISE EXCEPTION 'Invalid date range';
    END IF;

    -- Retrieve location type and metadata
    SELECT locationtype::TEXT, 
           locationname::TEXT, 
           usgs_id::TEXT, 
           wrnum::TEXT, 
           win::TEXT, 
           latitude::TEXT, 
           longitude::TEXT, 
           horizontalcoordrefsystem::TEXT,
           verticalmeasure::TEXT,
           stickup::TEXT,
           welldepth::TEXT,
           loggertype::TEXT,
           barologgertype::TEXT
    INTO v_metadata
    FROM gwportal.ugs_ngwmn_monitoring_locations
    WHERE altlocationid = p_well_id::integer;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Invalid well ID provided';
    END IF;

    v_location_type := v_metadata.locationtype;

    -- Return metadata
    RETURN QUERY
    SELECT 'Well Name'::TEXT AS metadata_key, v_metadata.locationname AS metadata_value, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
    UNION ALL SELECT 'USGS Number'::TEXT, '="' || COALESCE(v_metadata.usgs_id, '') || '"', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL SELECT 'Water Right Number'::TEXT, COALESCE(v_metadata.wrnum, ''), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL SELECT 'WIN'::TEXT, COALESCE(v_metadata.win, ''), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL SELECT 'Latitude ' || COALESCE('(' || v_metadata.horizontalcoordrefsystem || ')', ''), COALESCE(v_metadata.latitude, ''), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL SELECT 'Longitude ' || COALESCE('(' || v_metadata.horizontalcoordrefsystem || ')', ''), COALESCE(v_metadata.longitude, ''), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL SELECT 'UTM ' || COALESCE('(' || v_metadata.horizontalcoordrefsystem || ')', ''), ''::TEXT, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL SELECT 'Ground Elevation (ft amsl)'::TEXT, COALESCE(v_metadata.verticalmeasure, ''), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL SELECT 'Height of casing above ground surface (ft)'::TEXT, COALESCE(v_metadata.stickup, ''), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL SELECT 'Borehole depth (ft)'::TEXT, COALESCE(v_metadata.welldepth, ''), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL SELECT 'Logger type'::TEXT, COALESCE(v_metadata.loggertype, ''), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL SELECT 'Barometric logger'::TEXT, COALESCE(v_metadata.barologgertype, ''), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL;

    -- Build and execute query based on location type and aggregation
    IF lower(v_location_type) != 'spring' THEN
        v_sql := '
            SELECT 
                NULL::TEXT as metadata_key,
                NULL::TEXT as metadata_value,
                to_char(r.readingdate, ''DD/MM/YYYY'') as reading_date,
                r.measuredlevel::TEXT,
                r.temperature::TEXT,
                r.baroefficiencylevel::TEXT,
                r.measureddtw::TEXT,
                r.driftcorrection::TEXT,
                r.waterelevation::TEXT,
                NULL::TEXT as discharge,
                NULL::TEXT as comment
            FROM gwportal.reading r
            WHERE r.locationid = $1
              AND r.readingdate BETWEEN $2 AND $3
        ';

        IF p_type = 'daily' THEN
            v_sql := '
                SELECT 
                    NULL::TEXT as metadata_key,
                    NULL::TEXT as metadata_value,
                    to_char(date_trunc(''day'', r.readingdate), ''DD/MM/YYYY'') as reading_date,
                    AVG(r.measuredlevel)::TEXT as measuredlevel,
                    AVG(r.temperature)::TEXT as temperature,
                    AVG(r.baroefficiencylevel)::TEXT as baroefficiencylevel,
                    AVG(r.measureddtw)::TEXT as measureddtw,
                    AVG(r.driftcorrection)::TEXT as driftcorrection,
                    AVG(r.waterelevation)::TEXT as waterelevation,
                    NULL::TEXT as discharge,
                    NULL::TEXT as comment
                FROM gwportal.reading r
                WHERE r.locationid = $1
                  AND r.readingdate BETWEEN $2 AND $3
                GROUP BY date_trunc(''day'', r.readingdate)
            ';
        ELSIF p_type = 'monthly' THEN
            v_sql := '
                SELECT 
                    NULL::TEXT as metadata_key,
                    NULL::TEXT as metadata_value,
                    to_char(date_trunc(''month'', r.readingdate), ''DD/MM/YYYY'') as reading_date,
                    AVG(r.measuredlevel)::TEXT as measuredlevel,
                    AVG(r.temperature)::TEXT as temperature,
                    AVG(r.baroefficiencylevel)::TEXT as baroefficiencylevel,
                    AVG(r.measureddtw)::TEXT as measureddtw,
                    AVG(r.driftcorrection)::TEXT as driftcorrection,
                    AVG(r.waterelevation)::TEXT as waterelevation,
                    NULL::TEXT as discharge,
                    NULL::TEXT as comment
                FROM gwportal.reading r
                WHERE r.locationid = $1
                  AND r.readingdate BETWEEN $2 AND $3
                GROUP BY date_trunc(''month'', r.readingdate)
            ';
        END IF;
    ELSE
        v_sql := '
            SELECT 
                NULL::TEXT as metadata_key,
                NULL::TEXT as metadata_value,
                to_char(f.flowdate, ''DD/MM/YYYY'') as reading_date,
                NULL::TEXT as measuredlevel,
                NULL::TEXT as temperature,
                NULL::TEXT as baroefficiencylevel,
                NULL::TEXT as measureddtw,
                NULL::TEXT as driftcorrection,
                NULL::TEXT as waterelevation,
                f.discharge::TEXT,
                c.comment::TEXT
            FROM gwportal.ugs_gw_flow f
            LEFT JOIN gwportal.ugs_gw_comments c ON c.comment_id = f.comments
            WHERE f.locationid = $1
              AND f.flowdate BETWEEN $2 AND $3
        ';

        IF p_type = 'daily' THEN
            v_sql := '
                SELECT 
                    NULL::TEXT as metadata_key,
                    NULL::TEXT as metadata_value,
                    to_char(date_trunc(''day'', f.flowdate), ''DD/MM/YYYY'') as reading_date,
                    NULL::TEXT as measuredlevel,
                    NULL::TEXT as temperature,
                    NULL::TEXT as baroefficiencylevel,
                    NULL::TEXT as measureddtw,
                    NULL::TEXT as driftcorrection,
                    NULL::TEXT as waterelevation,
                    AVG(f.discharge)::TEXT as discharge,
                    string_agg(DISTINCT c.comment::TEXT, ''; '') as comment
                FROM gwportal.ugs_gw_flow f
                LEFT JOIN gwportal.ugs_gw_comments c ON c.comment_id = f.comments
                WHERE f.locationid = $1
                  AND f.flowdate BETWEEN $2 AND $3
                GROUP BY date_trunc(''day'', f.flowdate)
            ';
        ELSIF p_type = 'monthly' THEN
            v_sql := '
                SELECT 
                    NULL::TEXT as metadata_key,
                    NULL::TEXT as metadata_value,
                    to_char(date_trunc(''month'', f.flowdate), ''DD/MM/YYYY'') as reading_date,
                    NULL::TEXT as measuredlevel,
                    NULL::TEXT as temperature,
                    NULL::TEXT as baroefficiencylevel,
                    NULL::TEXT as measureddtw,
                    NULL::TEXT as driftcorrection,
                    NULL::TEXT as waterelevation,
                    AVG(f.discharge)::TEXT as discharge,
                    string_agg(DISTINCT c.comment::TEXT, ''; '') as comment
                FROM gwportal.ugs_gw_flow f
                LEFT JOIN gwportal.ugs_gw_comments c ON c.comment_id = f.comments
                WHERE f.locationid = $1
                  AND f.flowdate BETWEEN $2 AND $3
                GROUP BY date_trunc(''month'', f.flowdate)
            ';
        END IF;
    END IF;

    v_sql := v_sql || ' ORDER BY reading_date';

    -- Execute the query and return results
    RETURN QUERY EXECUTE v_sql USING p_well_id::integer, p_from_date, p_to_date;

    -- Check if no data was returned
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'No Data Found'::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT;
    END IF;
END;
$$ LANGUAGE plpgsql;