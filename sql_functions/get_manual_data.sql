CREATE OR REPLACE FUNCTION gwportal.get_manual_data(
   p_well_id TEXT
) RETURNS TABLE (
   metadata_key TEXT,
   metadata_value TEXT,
   reading_date TEXT,
   dtw_below_ground TEXT,
   water_elevation TEXT,
   data_status TEXT
) AS $$
DECLARE
   v_location_type TEXT;
   v_metadata RECORD;
BEGIN
   -- Get well metadata
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
   
   -- Return metadata rows first, then manual data sorted by reading_date
   RETURN QUERY
   SELECT 'Well Name'::TEXT AS metadata_key, 
          v_metadata.locationname AS metadata_value, 
          NULL::TEXT AS reading_date, 
          NULL::TEXT AS dtw_below_ground, 
          NULL::TEXT AS water_elevation, 
          NULL::TEXT AS data_status
   UNION ALL SELECT 'USGS Number'::TEXT, '="' || COALESCE(v_metadata.usgs_id, '') || '"', NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL SELECT 'Water Right Number'::TEXT, COALESCE(v_metadata.wrnum, ''), NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL SELECT 'WIN'::TEXT, COALESCE(v_metadata.win, ''), NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL SELECT 'Latitude ' || COALESCE('(' || v_metadata.horizontalcoordrefsystem || ')', ''), COALESCE(v_metadata.latitude, ''), NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL SELECT 'Longitude ' || COALESCE('(' || v_metadata.horizontalcoordrefsystem || ')', ''), COALESCE(v_metadata.longitude, ''), NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL SELECT 'UTM ' || COALESCE('(' || v_metadata.horizontalcoordrefsystem || ')', ''), ''::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL SELECT 'Ground Elevation (ft amsl)'::TEXT, COALESCE(v_metadata.verticalmeasure, ''), NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL SELECT 'Height of casing above ground surface (ft)'::TEXT, COALESCE(v_metadata.stickup, ''), NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL SELECT 'Borehole depth (ft)'::TEXT, COALESCE(v_metadata.welldepth, ''), NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL SELECT 'Logger type'::TEXT, COALESCE(v_metadata.loggertype, ''), NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL SELECT 'Barometric logger'::TEXT, COALESCE(v_metadata.barologgertype, ''), NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
   UNION ALL
   (SELECT NULL::TEXT as metadata_key,
           NULL::TEXT as metadata_value,
           to_char(m.readingdate, 'MM/DD/YYYY HH24:MI:SS') as reading_date,
           m.dtwbelowground::TEXT as dtw_below_ground,
           m.waterelevation::TEXT as water_elevation,
           m.datastatus::TEXT as data_status
    FROM gwportal.ugs_gw_manualdata m
    WHERE m.locationid = p_well_id::integer
    ORDER BY m.readingdate ASC);
END;
$$ LANGUAGE plpgsql;