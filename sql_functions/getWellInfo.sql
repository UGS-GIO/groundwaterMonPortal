CREATE OR REPLACE FUNCTION sde.get_well_info(p_loc_id TEXT)
RETURNS TEXT AS $$
DECLARE
    v_well_info TEXT;
    v_location_type TEXT;
BEGIN
    SELECT
        'Well Name,"' || locationname || '"' || E'\n' ||
        'USGS Number,"' || usgs_id || '"' || E'\n' ||
        'Water Right Number,' || wrnum || E'\n' ||
        'WIN,' || win || E'\n' ||
        'Latitude (' || COALESCE(horizontalcoordrefsystem, '') || '),' || latitude || E'\n' ||
        'Longitude (' || COALESCE(horizontalcoordrefsystem, '') || '),' || longitude || E'\n' ||
        'UTM (' || COALESCE(horizontalcoordrefsystem, '') || ')' || E'\n' ||
        'Ground Elevation (ft amsl),' || verticalmeasure || E'\n' ||
        'Height of casing above ground surface (ft),' || stickup || E'\n' ||
        'Borehole depth (ft),' || welldepth || E'\n' ||
        'Screened Interval (ft),' || COALESCE(l.minfrom, '') || '-' || COALESCE(l.maxto, '') || E'\n' ||
        'Screened Unit,' || COALESCE(la.aquifername, '') || E'\n' ||
        'Logger type,' || COALESCE(loggertype, '') || E'\n' ||
        'Barometric logger,' || COALESCE(barologgertype, '') || E'\n' ||
        'Barometric correction integrated in Global Water loggers and performed by software function in Solinst loggers' || E'\n' ||
        E'\n' ||
        'Description of Fields' || E'\n' ||
        CASE WHEN location_type != 'SPRING' THEN
            'Measured Level is the height of the water column (in feet) in the well above the logger' || E'\n' ||
            'Temp is the water temperature (in degrees C) measured by the logger (if logger is equipped with thermometer)' || E'\n' ||
            'Measured DTW is the depth to water (in feet) from the top of the casing; as measured by the logger or by manual tape measurement' || E'\n' ||
            'Baro Efficiency Level is the MeasuredDTW value with any applicable corrections applied for barometric effects and barometric efficiency' || E'\n' ||
            'Drift Correction is a correction (in feet) applied to compensate for the drift in instrument readings between manual tape measurements' || E'\n' ||
            'Water Elevation is the elevation (in feet) of the top of the water column (above mean sea level); this is the value used to generate our graphs' || E'\n' ||
            'Tape indicates if the measurement is a manual tape measurement (1) or a logger measurement (0)' || E'\n'
        ELSE
            'Discharge is in cubic feet per second' || E'\n'
        END
    INTO v_well_info
    FROM sde.UGS_NGWMN_MONITORING_LOCATIONS ml
    LEFT JOIN (
        SELECT siteno, MIN(lithologydepthfrom) as minfrom, MAX(lithologydepthto) as maxto
        FROM sde.UGS_NGWMN_LITHOLOGY
        GROUP BY siteno
    ) l ON l.siteno = ml.locationid
    LEFT JOIN sde.UGS_NGWMN_LOCAL_AQUIFER la ON la.code = ml.aquifername
    WHERE ml.objectid = p_loc_id::integer;

    RETURN v_well_info;
END;
$$ LANGUAGE plpgsql;