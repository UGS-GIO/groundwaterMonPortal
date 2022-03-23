<?php 

// for testing only, set $_POST variables
//ini_set('display_errors', 1);
//$_POST['wellIDs'] = '357';   //363 is a spring  //233 a well
//$_POST['type'] = 'daily';
//$_POST['fromdate'] = '3/30/2017';
//$_POST['todate'] = '12/7/2018';


// get db login info
require_once('dbconfig.php');

set_time_limit(300);

$allIDs = array();

$conn = pg_connect("host=$dbhost port=5432 dbname=$dbname user=$dbuser password=$dbpassword");

/*
LW Notes.  all mssql_ connection strings changed to their pg_ equivelents.
There's a lot of db prefixes in here that need to change.
*/

if(!$conn)
{
	die("Connection could not be established.");
}

if(isset($_POST['wellIDs'])){
	$wellIDs = $_POST['wellIDs'];
//	$allIDs = explode(",", $wellIDs);
}
else{
	die("Invalid well ID provided.");
}

if(isset($_POST['fromdate']) && isset($_POST['todate']))
{
	$fromdate = $_POST['fromdate'];
	$todate = $_POST['todate'];
}

if(strtotime($fromdate) > strtotime($todate))
{
	die("Invalid dates provided.");
}

if(isset($_POST['type']))
{
	$type = $_POST['type'];
}

if(isset($_POST['siteIDs']))
{
	$siteIDs = $_POST['siteIDs'];
	$sql = "SELECT OBJECTID FROM UGS_NGWMN_MONITORING_LOCATIONS WHERE SiteID IN (" . $siteIDs . ");";
	
	/// $res = mssql_query($sql, $conn);
	$res = pg_query($conn, $sql);

	if($res === false)
	{
		die("Error querying data1.");
	}

	//$row = sqlsrv_fetch_array( $res, SQLSRV_FETCH_ASSOC);
	/// while($row = mssql_fetch_array($res)){
	while ($row = pg_fetch_array($res)) {
		array_push($allIDs, $row['OBJECTID']);
	}
}

makeCSV($wellIDs);

exit;

function makeCSV($wellIDs){
    global $conn, $datatype, $range, $format, $fromdate, $todate, $type;
    // The header of the CSV file.
	$params = array();
	$files = array();
	// old mssql call
	//$sql = "SELECT AltLocationID, LocationType, HorizontalCoordRefSystem, LocationName, USGS_ID, WRNum, WIN, Latitude, Longitude, VerticalMeasure, Offset, WELLDEPTH, Name, BaroEfficiency, LoggerType, CAST(BaroEfficiencyStart as varchar(19)), BaroLoggerType, CAST(CONVERT(numeric(20,2), MIN(LithologyDepthFrom)) as NVARCHAR(50)) as MinFrom, CAST(CONVERT(numeric(20,2), MAX(LithologyDepthTo)) as NVARCHAR(50)) as MaxTo, LithologyDepthToUnit FROM UGS_NGWMN_MONITORING_LOCATIONS ml left join UGS_NGWMN_LITHOLOGY l on SiteNo = LocationID LEFT JOIN UGS_NGWMN_LOCAL_AQUIFER LA ON Code = AquiferName WHERE OBJECTID IN (". $wellIDs . ")";
	// new
	// wrong atempt #1$sql = "SELECT altlocationid, locationType, horizontalcoordrefsystem, locationname, usgs_id, wrnum, win, latitude, longitude, verticalmeasure, stickup as offset, welldepth, baroefficiency, loggertype, aquifername as name, to_char(baroefficiencystart,'MM/DD/YYYY'), BaroLoggerType, MinFrom, MaxTo FROM ( SELECT CAST(siteno as VARCHAR(50)), CAST(MIN(lithologydepthfrom) as VARCHAR(50)) as MinFrom, CAST(MAX(lithologydepthto) as VARCHAR(50)) as MaxTo FROM ugs_ngwmn_lithology GROUP BY siteno) l RIGHT JOIN ugs_ngwmn_monitoring_locations ON siteno = locationid WHERE OBJECTID IN(". $wellIDs . ")";
	$sql = "SELECT altlocationid, locationType, horizontalcoordrefsystem, locationname, usgs_id, wrnum, win, latitude, longitude, verticalmeasure, stickup as offset, ml.objectid, welldepth, baroefficiency, loggertype, la.aquifername as Name, to_char(baroefficiencystart,'MM/DD/YYYY'), BaroLoggerType, CAST(siteno as VARCHAR(50)), CAST(MIN(lithologydepthfrom) as VARCHAR(50)) as MinFrom, CAST(MAX(lithologydepthto) as VARCHAR(50)) as MaxTo, LithologyDepthToUnit FROM UGS_NGWMN_MONITORING_LOCATIONS ml left join UGS_NGWMN_LITHOLOGY l on SiteNo = LocationID LEFT JOIN UGS_NGWMN_LOCAL_AQUIFER LA ON Code = la.aquifername WHERE ml.objectid IN (". $wellIDs . ")";

	// this shouldn't have changed?  test & see if it needs to be converted too.
	//$sql .= " GROUP BY AltLocationID, LocationType, HorizontalCoordRefSystem, LocationName, USGS_ID, WRNum, WIN, Latitude, Longitude, VerticalMeasure, Offset, WELLDEPTH, Name, BaroEfficiency, LoggerType, BaroEfficiencyStart, BaroLoggerType, LithologyDepthToUnit;";
	//$sql .= " GROuP BY AltLocationID, LocationType, HorizontalCoordRefSystem, LocationName, USGS_ID, WRNum, WIN, Latitude, Longitude, VerticalMeasure, stickup, WELLDEPTH, Name, BaroEfficiency, LoggerType, BaroEfficiencyStart, BaroLoggerType, l.minfrom,l.maxto;";	//I took out LithologyDepthToUnit because it errored
	$sql .= " GROUP BY altlocationid, ml.locationtype,ml.locationid, ml.horizontalcoordrefsystem, locationname,l.siteno,l.lithologydepthtounit,ml.objectid, la.aquifername, USGS_ID, WRNum, WIN, Latitude, Longitude, VerticalMeasure, stickup, WELLDEPTH, BaroEfficiency, LoggerType, BaroEfficiencyStart, BaroLoggerType;";

	/// $res = mssql_query($sql, $conn);
	$res = pg_query($conn, $sql);

	if($res === false)
	{
		//die("Error querying data.");
		die(pg_last_error ());
	}

	/// while($row = mssql_fetch_array($res)){
	while ($row = pg_fetch_array($res)) {
		$locID = $row['altlocationid'];
		$dataFile = $row['locationtype'];
		$coordSys = $row['horizontalcoordrefsystem'];
		$screenInterval = "";

		if($coordSys == null || $coordSys == "")
			$coordSys = "";
		else
			$coordSys = "(" . $coordSys . ")";
		
		if($row['minfrom'] != null && $row['maxto'] != null)
			$screenInterval = $row['minfrom'] . "-" . $row['maxto'];
		
		$string = "";
		$sql = "";
		/* $string .= "Well Name,=\"" . $row['LocationName'] . "\"\r\n" .
					"USGS Number,=\"" . $row['USGS_ID'] . "\"\r\n" .
					"Water Right Number," . $row['WRNum'] . "\r\n" .
					"WIN," . $row['WIN'] . "\r\n" .
					"Latitude " . $coordSys . "," . $row['Latitude'] . "\r\n" .
					"Longitude " . $coordSys . "," . $row['Longitude'] . "\r\n" .
					"UTM " . $coordSys . "\r\n" . 
					"Ground Elevation (ft amsl)," . $row['VerticalMeasure'] . "\r\n" .
					"Height of casing above ground surface (ft)," . $row['Offset'] . "\r\n" .
					"Borehole depth (ft)," . $row['WellDepth'] . "\r\n" .
					"Screened Interval (ft), " . $screenInterval . "\r\n" .
					"Screened Unit, " . ($row['Name'] != null ? $row['Name'] : "") . "\r\n" . //$screenedunit['Description'] . "\r\n" .
					//"Barometric Efficiency," . $row['BaroEfficiency'] . "\r\n" .
					"Completion date," . $row['DateCompleted'] . "\r\n" . 
					"Logger type," . $row['LoggerType'] . "\r\n" .
					"Barometric logger," . $barologger['BaroLoggerType'] . "\r\n" . 
					//"Start of barometric Efficiency," . $row['BaroEfficiencyStart'] . "\r\n" .
					"Barometric correction integrated in Global Water loggers and performed by software function in Solinst loggers\r\n" .
					"\r\n" .
					"Description of Fields\r\n"; */
					
		if(strtolower($dataFile) != 'spring')
		{
			$string .= "Well Name,=\"" . $row['locationname'] . "\"\r\n" .
					"USGS Number,=\"" . $row['usgs_id'] . "\"\r\n" .
					"Water Right Number," . $row['wrnum'] . "\r\n" .
					"WIN," . $row['win'] . "\r\n" .
					"Latitude " . $coordSys . "," . $row['latitude'] . "\r\n" .
					"Longitude " . $coordSys . "," . $row['longitude'] . "\r\n" .
					"UTM " . $coordSys . "\r\n" . 
					"Ground Elevation (ft amsl)," . $row['verticalmeasure'] . "\r\n" .
					"Height of casing above ground surface (ft)," . $row['offset'] . "\r\n" .
					"Borehole depth (ft)," . $row['welldepth'] . "\r\n" .
					"Screened Interval (ft), " . $screenInterval . "\r\n" .
					"Screened Unit, " . ($row['name'] != null ? $row['name'] : "") . "\r\n" . //$screenedunit['Description'] . "\r\n" .
					//"Barometric Efficiency," . $row['BaroEfficiency'] . "\r\n" .
					"Completion date," . $row['datecompleted'] . "\r\n" . 
					"Logger type," . $row['loggertype'] . "\r\n" .
					"Barometric logger," . $barologger['barologgertype'] . "\r\n" . 
					//"Start of barometric Efficiency," . $row['BaroEfficiencyStart'] . "\r\n" .
					"Barometric correction integrated in Global Water loggers and performed by software function in Solinst loggers\r\n" .
					"\r\n" .
					"Description of Fields\r\n" .
					"Measured Level is the height of the water column (in feet) in the well above the logger\r\n" .
					"Temp is the water temperature (in degrees C) measured by the logger (if logger is equipped with thermometer)\r\n" . 
					//"DeltaLevel is the difference (in feet) between the current logger measurement and the initial logger measurement of the logging set (used for calculations)\r\n" .
					"Measured DTW is the depth to water (in feet) from the top of the casing; as measured by the logger or by manual tape measurement\r\n" . 
					"Baro Efficiency Level is the MeasuredDTW value with any applicable corrections applied for barometric effects and barometric efficiency\r\n" .
					"Drift Correction is a correction (in feet) applied to compensate for the drift in instrument readings between manual tape measurements\r\n" . 
					//"DTWBelowCasing is the depth to water (in feet) from the top of casing with the drift correction applied\r\n" . 
					//"DTWBelowGroundSurface is the depth to water (in feet) below the ground surface (the height of the casing has been subtracted from DTWBelowCasing)\r\n" . 
					"Water Elevation is the elevation (in feet) of the top of the water column (above mean sea level); this is the value used to generate our graphs\r\n" .
					"Tape indicates if the measurement is a manual tape measurement (1) or a logger measurement (0)\r\n";
		}
		else
		{
			$string .= "Name,=\"" . $row['locationname'] . "\"\r\n" .
					"USGS Number,=\"" . $row['usgs_id'] . "\"\r\n" .
					"Water Right Number," . $row['wrnum'] . "\r\n" .
					"Latitude " . $coordSys . "," . $row['latitude'] . "\r\n" .
					"Longitude " . $coordSys . "," . $row['longitude'] . "\r\n" .
					"UTM " . $coordSys . "\r\n" . 
					"Ground Elevation (ft amsl)," . $row['verticalmeasure'] . "\r\n" .
					"\r\n" .
					"Description of Fields\r\n" .
					"Discharge is in cubic feet per second\r\n";
		}

		if(strtolower($dataFile) != 'spring')
		//if(true)
		{
			if($type == 'all')
			{
				// old $sql = "SELECT '' as dtOrder, CONVERT(varchar(19),READINGDATE) as 'Reading Date', MEASUREDLEVEL as 'Measured Level', TEMP as 'Temp', BAROEFFICIENCYLEVEL as 'Baro Efficiency Level', MEASUREDDTW as 'Measured DTW', DRIFTCORRECTION as 'Drift Correction', WATERELEVATION as 'Water Elevation', TAPE as 'Tape' FROM UGS_GW_READING WHERE LOCATIONID = '" . $locID . "'";
				$sql = "SELECT '' as dtOrder, CAST(readingdate as varchar(19)) as \"Reading Date\", MEASUREDLEVEL as \"Measured Level\", Temperature as Temp, BAROEFFICIENCYLEVEL as \"Baro Efficiency Level\", MEASUREDDTW as \"Measured DTW\", DRIFTCORRECTION as \"Drift Correction\", WATERELEVATION as \"Water Elevation\" FROM reading WHERE LOCATIONID = '" . $locID . "'"; 
				
				//
				// Setup
				if(strtotime($fromdate) < strtotime($todate))
				{
					$sql .= " AND READINGDATE BETWEEN CAST('" . $fromdate ."' AS DATE) AND CAST('" . $todate . "' AS DATE)";
				}
				
				$sql .= " ORDER BY READINGDATE;";
			}
			else if($type == 'daily')
			{
				//$sql = "SELECT CAST(READINGDATE as Date) AS dtOrder, TO_CHAR(READINGDATE, 'DD/MM/YYYY') as \"Reading Date\", Sum(MEASUREDLEVEL) / COUNT(CAST(READINGDATE as Date)) as \"Measured Level\", Sum(TEMPERATURE) / COUNT(CAST(READINGDATE as Date)) as TEMPERATURE, Sum(BAROEFFICIENCYLEVEL) / COUNT(CAST(READINGDATE as Date)) as \"Barometric Efficiency Level\", Sum(MEASUREDDTW) / COUNT(CAST(READINGDATE as Date)) as \"Measured DTW\", Sum(DRIFTCORRECTION) / COUNT(CAST(READINGDATE as Date)) as \"Drift Correction\", Sum(WATERELEVATION) / COUNT(CAST(READINGDATE as Date)) as \"Water Elevation\" from UGS_GW_READING where LOCATIONId = '" . $locID . "' GROUP BY dtorder, \"Reading Date\"";
				$sql = "SELECT CAST(READINGDATE as Date) AS dtOrder, TO_CHAR(READINGDATE, 'DD/MM/YYYY') as \"Reading Date\", Sum(MEASUREDLEVEL) / COUNT(CAST(READINGDATE as Date)) as \"Measured Level\", Sum(TEMPERATURE) / COUNT(CAST(READINGDATE as Date)) as TEMPERATURE, Sum(BAROEFFICIENCYLEVEL) / COUNT(CAST(READINGDATE as Date)) as \"Barometric Efficiency Level\", Sum(MEASUREDDTW) / COUNT(CAST(READINGDATE as Date)) as \"Measured DTW\", Sum(DRIFTCORRECTION) / COUNT(CAST(READINGDATE as Date)) as \"Drift Correction\", Sum(WATERELEVATION) / COUNT(CAST(READINGDATE as Date)) as \"Water Elevation\" from reading where LOCATIONId = '" . $locID . "'";
				//
				// Setup
				if(strtotime($fromdate) < strtotime($todate))
				{
					$sql .= " AND READINGDATE BETWEEN CAST('" . $fromdate ."' AS DATE) AND CAST('" . $todate . "' AS DATE)";
				}
				
				$sql .= " GROUP by TO_CHAR(READINGDATE, 'DD/MM/YYYY'), CAST(READINGDATE as Date) ORDER BY CAST(READINGDATE as date);";			
			}
			else if($type == 'monthly')
			{
				//old $sql = "SELECT CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00'+ CAST (MONTH(READINGDATE) AS varchar), 2) as dtOrder, +CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar) || '/' || +CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) as 'Reading Date', Sum(MEASUREDLEVEL) / COUNT(+CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00'+ CAST (MONTH(READINGDATE) AS varchar), 2)) as 'Measured Level', Sum(TEMP) / COUNT(+CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00'+ CAST (MONTH(READINGDATE) AS varchar), 2)) as Temp, Sum(BAROEFFICIENCYLEVEL) / COUNT(+CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00'+ CAST (MONTH(READINGDATE) AS varchar), 2)) as 'Barometric Efficiency Level',  Sum(MEASUREDDTW) / COUNT(+CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00'+ CAST (MONTH(READINGDATE) AS varchar), 2)) as 'Measured DTW', Sum(DRIFTCORRECTION) / COUNT(+CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00'+ CAST (MONTH(READINGDATE) AS varchar), 2)) as 'Drift Correction', Sum(WATERELEVATION) / COUNT(+CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00'+ CAST (MONTH(READINGDATE) AS varchar), 2)) as 'Water Elevation' from UGGPADMIN.UGS_GW_READING where LOCATIONID = '" . $locID . "'";
				      $sql = "SELECT CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00' || CAST (EXTRACT(MONTH FROM READINGDATE) AS varchar), 2) as dtOrder, CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar) || '/' || CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) as \"Reading Date\", ROUND(Sum(MEASUREDLEVEL) / COUNT(CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar), 2)),3) as \"Measured Level\", ROUND(Sum(Temperature) / COUNT(CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar), 2)),3) as Temp, ROUND(Sum(BAROEFFICIENCYLEVEL) / COUNT(CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar), 2)),3) as \"Barometric Efficiency Level\", ROUND(Sum(MEASUREDDTW) / COUNT(CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT('00' || CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar), 2)),3) as \"Measured DTW\", ROUND(Sum(DRIFTCORRECTION) / COUNT(CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar), 2)),5) as \"Drift Correction\", ROUND(Sum(WATERELEVATION) / COUNT(CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT('00' || CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar), 2)),3) as \"Water Elevation\" from reading where LOCATIONID = '" . $locID . "'";

				// Setup
				if(strtotime($fromdate) < strtotime($todate))
				{
					$sql .= " AND READINGDATE BETWEEN CAST('" . $fromdate ."' AS DATE) AND CAST('" . $todate . "' AS DATE)";
				}
				
				//$sql .= " GROUP by CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00'+ CAST (MONTH(READINGDATE) AS varchar), 2), +CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar) || '/' || +CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) order by dtOrder;";
				  $sql .= " GROUP by CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) || '-' || RIGHT ('00' || CAST (EXTRACT(MONTH FROM READINGDATE) AS varchar), 2), CAST(EXTRACT(MONTH FROM READINGDATE) AS varchar) || '/' || CAST(EXTRACT(YEAR FROM READINGDATE) AS varchar) order by dtOrder;";
			}
		}
		else if(strtolower($dataFile) == 'spring')
		{
			if($type == "all")
			{
				//old $sql = "SELECT TO_CHAR(readingdate,'YYYY-MM-DD HH24:MI:SS') as FlowDate, Discharge, c.Comment from UGS_GW_FLOW f left join UGGPADMIN.UGS_GW_COMMENTS c on c.CommentID = f.COMMENT where LOCATIONID = '" . $locID . "'";
				$sql = "SELECT TO_CHAR(flowdate,'YYYY-MM-DD HH24:MI:SS') as FlowDate, Discharge, c.Comment from UGS_GW_FLOW f left join UGS_GW_COMMENTS c on c.Comment_ID = f.COMMENTS where LOCATIONID = '" . $locID . "'";

				if(strtotime($fromdate) < strtotime($todate))
				{
					$sql .= " AND FLOWDATE BETWEEN CAST('" . $fromdate ."' AS DATE) AND CAST('" . $todate . "' AS DATE)";
				}
				
				$sql .= " ORDER BY CAST(FLOWDATE as date);";
			}
			if($type == "daily")
			{
				$sql = "SELECT TO_CHAR(FLOWDATE, 'DD/MM/YYYY') as FlowDate, CAST(FLOWDATE as Date) AS dtOrder, Sum(Discharge) / COUNT(CAST(FLOWDATE as Date)) as Discharge, c.Comment from UGS_GW_FLOW f left join UGS_GW_COMMENTS c ON c.Comment_ID = f.COMMENTS where LOCATIONID = '" . $locID . "'";
				
				if(strtotime($fromdate) < strtotime($todate))
				{
					$sql .= " AND FLOWDATE BETWEEN CAST('" . $fromdate ."' AS DATE) AND CAST('" . $todate . "' AS DATE)";
				}
				
				$sql .= " group by TO_CHAR(FLOWDATE, 'DD/MM/YYYY'), CAST(FLOWDATE as Date), c.Comment ORDER BY CAST(FLOWDATE as date);";
			}
			else if($type == "monthly")
			{
				//old $sql = "SELECT CAST(EXTRACT(MONTH FROM FLOWDATE) AS varchar) || '/' || CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar) as FlowDate, CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar) || '-' || RIGHT ('00'+ CAST (MONTH(FLOWDATE) AS varchar), 2) as dtOrder, Sum(Discharge) / COUNT(+CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar) || '-' || RIGHT ('00'+ CAST (MONTH(FLOWDATE) AS varchar), 2)) as Discharge, c.Comment from UGS_GW_FLOW f left join UGGPADMIN.UGS_GW_COMMENTS c ON c.CommentID = f.COMMENT where LOCATIONID = '" . $locID . "'";
				      $sql = "SELECT CAST(EXTRACT(MONTH FROM FLOWDATE) AS varchar) || '/' || CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar) as FlowDate, CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH from FLOWDATE) AS varchar), 2) as dtOrder, Sum(Discharge) / COUNT(CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH FROM FLOWDATE) AS varchar), 2)) as Discharge, c.comment from UGS_GW_FLOW f left join UGS_GW_COMMENTS c ON c.comment_id = f.comments where LOCATIONID = '" . $locID . "'";
				
				if(strtotime($fromdate) < strtotime($todate))
				{
					$sql .= " AND FLOWDATE BETWEEN CAST('" . $fromdate ."' AS DATE) AND CAST('" . $todate . "' AS DATE)";
				}
				
				//old $sql .= " group by CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar) || '-' || RIGHT ('00' CAST (MONTH(FLOWDATE) AS varchar), 2), CAST(EXTRACT(MONTH FROM FLOWDATE) AS varchar) || '/' || CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar), c.Comment order by dtOrder;";
				$sql .= " group by CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH FROM FLOWDATE) AS varchar), 2), CAST(EXTRACT(MONTH FROM FLOWDATE) AS varchar) || '/' || CAST(EXTRACT(YEAR FROM FLOWDATE) AS varchar), c.comment  order by dtOrder;";
			}
		}
		else
		{
			$sql = "";
		}
	
		$filename = "temp/" . $row['locationname'] . ".csv";
		//$fh = fopen($filename, 'w') or die("Cannot open file for writing.");
		$fh = fopen($filename, 'w') or die("Cannot open file for writing.");

		/// $res2 = mssql_query($sql, $conn);
		$res2 = pg_query($conn, $sql);
//echo "<br>sql query: ".$sql;
		if($res2 === false)
		{
			die(pg_last_error ());
		} 
		
		if(pg_num_fields($res2) <= 0)
		{
			die("No array");
		}
		fwrite($fh, $string."\r\n");

		if(strtolower($dataFile) == 'spring')
		{
			fwrite($fh, "Flow Date,Discharge,Comment\r\n");
//echo "<br><br>About to write SPRING data<br>";			
			while($row2 = pg_fetch_array($res2)){
//echo '<pre>'; echo print_r($row2); echo '</pre>';
				fwrite($fh, $row2["flowdate"] . "," . $row2["discharge"] . "," . $row2["comment"] . "\r\n");
			}
		}
		else
		{
			for($i = 1; $i < pg_num_fields($res2); $i++)
			{
				fwrite($fh, pg_field_name($res2, $i) . ",");
			}
			fwrite($fh, "\r\n");
//echo "<br><br>About to write data for non spring<br>";
			// Data
			while($row2 = pg_fetch_array($res2)){
				for($i = 1; $i < pg_num_fields($res2); $i++)
				{
//echo '<pre>'; echo print_r($row2); echo '</pre>';
					fwrite($fh, $row2[$i] . ",");
				}
				fwrite($fh, "\r\n");
			}
		}
		fclose($fh);
		array_push($files, $filename);
	}

	if(count($files) > 1)
	{
		$zipname = "temp/" . uniqid("GWD", false) . ".zip";
		$zip = new ZipArchive;
		$zip->open($zipname, ZIPARCHIVE::CREATE);
		foreach($files as $file)
		{
			$zip->addFile($file);
		}
		$zip->close();
		$filename = $zipname;
		
		foreach($files as $file)
		{
			unlink($file);
		}
	}
	echo $filename;

	// free memory
	pg_free_result($res2);

	// close connection
	pg_close($conn);
	exit;
}
?>