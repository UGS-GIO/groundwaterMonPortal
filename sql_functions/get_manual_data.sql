CREATE OR REPLACE FUNCTION gwportal.get_manual_data(
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
   measured_dtw TEXT,
   drift_correction TEXT,
   water_elevation TEXT,
   discharge TEXT,
   comment TEXT,
   manual_elevation TEXT
) AS $$
DECLARE
   v_sql TEXT;
   v_location_type TEXT;
   v_metadata RECORD;
BEGIN
   IF p_from_date > p_to_date THEN
       RAISE EXCEPTION 'Invalid date range';
   END IF;

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

   -- Manual data queries for both wells and springs
   IF p_type = 'daily' THEN
       v_sql := '
           SELECT 
               NULL::TEXT as metadata_key,
               NULL::TEXT as metadata_value,
               to_char(date_trunc(''day'', m.readingdate), ''MM/DD/YYYY'') as reading_date,
               NULL::TEXT as measured_level,
               NULL::TEXT as temperature,
               NULL::TEXT as measured_dtw,
               NULL::TEXT as drift_correction,
               NULL::TEXT as water_elevation,
               NULL::TEXT as discharge,
               NULL::TEXT as comment,
               ROUND(AVG(m.waterelevation)::numeric, 8)::TEXT as manual_elevation
           FROM gwportal.ugs_gw_manualdata m
           WHERE m.locationid = $1
             AND m.readingdate BETWEEN $2 AND $3
           GROUP BY date_trunc(''day'', m.readingdate)
           ORDER BY date_trunc(''day'', m.readingdate) DESC
       ';
   ELSIF p_type = 'monthly' THEN
       v_sql := '
           SELECT 
               NULL::TEXT as metadata_key,
               NULL::TEXT as metadata_value,
               to_char(date_trunc(''month'', m.readingdate), ''MM/DD/YYYY'') as reading_date,
               NULL::TEXT as measured_level,
               NULL::TEXT as temperature,
               NULL::TEXT as measured_dtw,
               NULL::TEXT as drift_correction,
               NULL::TEXT as water_elevation,
               NULL::TEXT as discharge,
               NULL::TEXT as comment,
               ROUND(AVG(m.waterelevation), 8)::TEXT as manual_elevation
           FROM gwportal.ugs_gw_manualdata m
           WHERE m.locationid = $1
             AND m.readingdate BETWEEN $2 AND $3
           GROUP BY date_trunc(''month'', m.readingdate)
           ORDER BY date_trunc(''month'', m.readingdate) DESC
       ';
   ELSE
       v_sql := '
           SELECT 
               NULL::TEXT as metadata_key,
               NULL::TEXT as metadata_value,
               to_char(m.readingdate, ''MM/DD/YYYY HH24:MI:SS'') as reading_date,
               NULL::TEXT as measured_level,
               NULL::TEXT as temperature,
               NULL::TEXT as measured_dtw,
               NULL::TEXT as drift_correction,
               NULL::TEXT as water_elevation,
               NULL::TEXT as discharge,
               NULL::TEXT as comment,
               m.waterelevation::TEXT as manual_elevation
           FROM gwportal.ugs_gw_manualdata m
           WHERE m.locationid = $1
             AND m.readingdate BETWEEN $2 AND $3
           ORDER BY m.readingdate DESC
       ';
   END IF;

   RETURN QUERY EXECUTE v_sql USING p_well_id::integer, p_from_date, p_to_date;

   IF NOT FOUND THEN
       RETURN QUERY SELECT 'No Data Found'::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT;
   END IF;
END;
$$ LANGUAGE plpgsql;