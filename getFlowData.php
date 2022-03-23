<?php 

	// for testing only, set $_POST variables
	ini_set('display_errors', 1);
	//$_POST['locID'] = '10007';
	//$_POST['type'] = 'monthly';

	// get db login info
	require_once('dbconfig.php');

	$locIDs = "";
	
	// PostgresSQL CONNECTION
	$conn = pg_connect("host=$dbhost port=5432 dbname=$dbname user=$dbuser password=$dbpassword");

	if(!$conn)
	{
		die("Connection could not be established.");
	}

	//if(!pg_select($conn, $dbname))
	//{
	//	die("Could not select database.");
	//} 

	if(isset($_POST['locID'])){
		$locID = $_POST['locID'];
	}
	else{
		die("Invalid well ID provided.");
	}

	if(isset($_POST['type'])){
		$type = $_POST['type'];
	}
	else {
		die("Invalid type provided.");
	}

	
	$temp = explode(",", $locID);
	for($i = 0; $i < count($temp); $i++)
	{
		$locIDs .= "'" . $temp[$i] . "',";
	}
	$locIDs = rtrim($locIDs, ',');

	if($type == "daily")
	{
		// OLD $sql = "SELECT CONVERT(VARCHAR(10), FLOWDATE, 101) as FlowDate, CAST(FLOWDATE as Date) AS dtOrder, Sum(Discharge) / COUNT(CAST(FLOWDATE as Date)) as Discharge, LOCATIONID as LocationID from UGGPADMIN.UGS_GW_FLOW where LOCATIONID IN (" . $locIDs . ") group by CONVERT(VARCHAR(10), FLOWDATE, 101), CAST(FLOWDATE as Date), LOCATIONID ORDER BY CAST(FLOWDATE as date);";
		//$sql = "SELECT to_char(FLOWDATE, 'MM/DD/YYYY') as FlowDate, CAST(FLOWDATE as Date) AS dtOrder, TRUNC(Sum(Discharge) / COUNT(CAST(FLOWDATE as Date)),6) as Discharge, LOCATIONID as LocationID from UGS_GW_FLOW where LOCATIONID IN (" . $locIDs . ") group by to_char(FLOWDATE, 'MM/DD/YYYY'), CAST(FLOWDATE as Date), LOCATIONID ORDER BY CAST(FLOWDATE as date);";
		$sql = "SELECT to_char(FLOWDATE, 'MM/DD/YYYY') as FlowDate, TRUNC(Sum(Discharge) / COUNT(CAST(FLOWDATE as Date)),4) as Discharge from UGS_GW_FLOW where LOCATIONID IN (" . $locIDs . ") group by to_char(FLOWDATE, 'MM/DD/YYYY'), CAST(FLOWDATE as Date) ORDER BY CAST(FLOWDATE as date);";
	}
	else if($type == "monthly")
	{
		//old $sql = "SELECT CAST(MONTH(FLOWDATE) AS varchar) + '/' + CAST(YEAR(FLOWDATE) AS varchar) as FlowDate, CAST(YEAR(FLOWDATE) AS varchar) + '-' + RIGHT ('00'+ CAST (MONTH(FLOWDATE) AS varchar), 2) as dtOrder, Sum(Discharge) / COUNT(CAST(YEAR(FLOWDATE) AS varchar) + '-' + RIGHT ('00'+ CAST (MONTH(FLOWDATE) AS varchar), 2)) as Discharge, LOCATIONID as LocationID from UGGPADMIN.UGS_GW_FLOW where LOCATIONID IN (" . $locIDs . ") group by CAST(YEAR(FLOWDATE) AS varchar) + '-' + RIGHT ('00'+ CAST (MONTH(FLOWDATE) AS varchar), 2), CAST(MONTH(FLOWDATE) AS varchar) + '/' + CAST(YEAR(FLOWDATE) AS varchar), LOCATIONID order by dtOrder;";
		//$sql = "SELECT CAST(EXTRACT(MONTH FROM flowdate) as varchar) || '/' || CAST(EXTRACT(YEAR FROM flowdate) AS varchar) as FlowDate, CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2) as dtOrder, TRUNC(Sum(Discharge) / COUNT(CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2)),4) as Discharge, LOCATIONID as LocationID from UGS_GW_FLOW where LOCATIONID IN (10010) group by CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || '-' || RIGHT('00' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2), CAST(EXTRACT(MONTH FROM flowdate) AS varchar) || '/' || CAST(EXTRACT(YEAR FROM flowdate) AS varchar), LOCATIONID order by dtOrder;";
		$sql = "SELECT CAST(EXTRACT(MONTH FROM flowdate) as varchar) || '/' || CAST(EXTRACT(YEAR FROM flowdate) AS varchar) as FlowDate, CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2) as dtOrder, TRUNC(Sum(Discharge) / COUNT(CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || '-' || RIGHT ('00' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2)),4) as Discharge from UGS_GW_FLOW where LOCATIONID IN (10010) group by CAST(EXTRACT(YEAR FROM flowdate) AS varchar) || '-' || RIGHT('00' || CAST(EXTRACT(MONTH FROM flowdate) AS varchar), 2), CAST(EXTRACT(MONTH FROM flowdate) AS varchar) || '/' || CAST(EXTRACT(YEAR FROM flowdate) AS varchar) order by dtOrder;";
	}
	else
	{
		die("Invalid type provided.");
	}
	

	/// $res = mssql_query($sql);
	$res = pg_query($conn, $sql);

	if($res === false)
	{
		die(pg_last_error ()); 
		//die("Error querying data.");
	}

	$data_arr = pg_fetch_all($res);

	//print "<pre>";
	//print_r ($data_arr);
	//print "</pre>";

	print json_encode($data_arr);

	// free memory
	pg_free_result($res);

	// close connection
	pg_close($conn);

	exit;

?>
