<?php
/*
This script prepares payment data for further analysis.  
It calculates running totals.

back_fill
*/
$xml = 0;
$text = 1;
$textAppend = 0;
$db = 0;
$output_filename = "payment_quality_devel";
$delimeter = ";";

echo "Launching...\n";

// Import SQL query
$sqlString = file_get_contents("./payment_data.sql");

echo "Connecting to DB...\n";
// Connect to DB for query
$dbQ = new PDO(getenv('db_cs'), getenv('db_user'), getenv('db_pass'));
$dbQ->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

if($db) {
	// Connect to DB for write
	$dbW = new PDO(getenv('db_cs'), getenv('db_user'), getenv('db_pass'));
	$dbW->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
}

echo "Opening output file...\n";
// Open output file
$openMethod = ($textAppend) ? "a" : "w";
if($text) if(! $handle = fopen("./".$output_filename.".txt", $openMethod)) throw new Exception("Could not open output file.");

if($xml) {
	$xw = new XMLWriter();
	$xw->openMemory();
	$xw->setIndent(1);
	$xw->startDocument('1.0');
	//$xw->openURI('./'.$output_filename.'.xml');
	$xw->startElement("data"); 
}

echo "Launching query...\n";
$stmt = $dbQ->query($sqlString);

// Initialize vars
$k = 0;
$paymentsRT = 0;
$possiblePaymentsRT = 0;
$revenueRT = 0;
$rpsRT = 0;
$lastSource = '';
$lastCohort='';
$lastPayWeek = '';
$lastProduct = '';
$paymentRevenue = 2.4;
$headers = array();
$headersExtra = array();
$headersExtra[] = 'revenue';
$headersExtra[] = 'rps';
$headersExtra[] = 'paymentsRT';
$headersExtra[] = 'possiblePaymentsRT';
$headersExtra[] = 'paymentsRT1';
$headersExtra[] = 'paymentsRT2';
$headersExtra[] = 'paymentsRT3';
$headersExtra[] = 'paymentsRT4';
$headersExtra[] = 'possiblePaymentsRT1';
$headersExtra[] = 'possiblePaymentsRT2';
$headersExtra[] = 'possiblePaymentsRT3';
$headersExtra[] = 'possiblePaymentsRT4';
$headersExtra[] = 'revenueRT';
$headersExtra[] = 'rpsRT';

echo "Looping through results...\n";
// Loop through results
while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
	$r++;
	echo "Processing row $r ...\n";
	if($xml) $xw->startElement("row");
	
	// Write file headers if this is first row
	if(''==$lastSource) {
		$colcount = $stmt->columnCount();
		for($a=0;$a<$colcount;$a++) {
			$meta = $stmt->getColumnMeta($a);
			$headers[] = $meta['name'];
		}
		
		// Add headers and custom headers
		if($text && !$textAppend) if(! fwrite ($handle, implode($delimeter, array_merge($headers,$headersExtra)) . "\n")) 
			throw new Exception("Could not write header to output file.");
		
		if($db) {
			$cols = implode(",", array_merge($headers,$headersExtra));
			$vals = implode(",:", array_merge($headers,$headersExtra));
			$cols = "($cols)";
			$vals = "(:$vals)";
			$insert = $dbW->prepare("INSERT INTO payment_quality $cols VALUES $vals");
		}
	}
	
	$row['revenue'] = $row['payments'] * $paymentRevenue;
	$row['rps'] = $row['revenue'] / $row['originalSubscribers'];
	
	// Same cohort
	if($lastSource == strtoupper($row["source"]) && $lastCohort == $row["cohort"] && $lastProduct == strtoupper($row["product"])) {
		$paymentsRT += $row['payments'];
		$possiblePaymentsRT += $row['possiblePayments'];
		$revenueRT += $row['revenue'];
		$rpsRT += $row['rps'];
		
		switch($row['weeks']) {
			case 1:
				// We really shouldn't find ourselves in this position, only when there 
				// is funky data with payments before cohort date
				file_put_contents('php://stderr', "Existing problem cohort {$row['product']} {$row['source']} {$row['cohort']} {$row['payWeek']} {$row['weeks']}\n");
				$paymentPercent1 = $paymentsRT / $possiblePaymentsRT;
				$paymentsRT1 = $paymentsRT;
				$possiblePaymentsRT1 = $possiblePaymentsRT;				
			case 2:
				$paymentPercent2 = $paymentsRT / $possiblePaymentsRT;
				$paymentsRT2 = $paymentsRT;
				$possiblePaymentsRT2 = $possiblePaymentsRT;
				break;
			case 3:
				$paymentPercent3 = $paymentsRT / $possiblePaymentsRT;
				$paymentsRT3 = $paymentsRT;
				$possiblePaymentsRT3 = $possiblePaymentsRT;				
				break;
			case 4:
				$paymentPercent4 = $paymentsRT / $possiblePaymentsRT;
				$paymentsRT4 = $paymentsRT;
				$possiblePaymentsRT4 = $possiblePaymentsRT;				
				break;				
		}
	
	// New cohort
	} else {
		if ($row['weeks'] < 1) {
			$problemCohort = 1;
			file_put_contents('php://stderr', "New problem cohort {$row['product']} {$row['source']} {$row['cohort']} {$row['payWeek']} {$row['weeks']}\n");
		} else {
			$problemCohort = 0;			
		} 
			
		$paymentsRT = $row['payments'];
		$possiblePaymentsRT = $row['possiblePayments'];
		$paymentsRT1 = $paymentsRT;
		$possiblePaymentsRT1 = $possiblePaymentsRT;
		$paymentsRT2 = 0;
		$possiblePaymentsRT2 = 0;		
		$paymentsRT3 = 0;
		$possiblePaymentsRT3 = 0;	
		$paymentsRT4 = 0;
		$possiblePaymentsRT4 = 0;	
		$paymentPercent2 = 0;
		$paymentPercent3 = 0;
		$paymentPercent4 = 0; 
		$revenueRT = $row['revenue'];
		$rpsRT = $row['rps'];	

	}
	
	// Write custom columns, ORDER IS IMPORTANT to match with headers
	$row['paymentsRT'] = $paymentsRT;
	$row['possiblePaymentsRT'] = $possiblePaymentsRT;
	$row['paymentsRT1'] = $paymentsRT1;
	$row['paymentsRT2'] = $paymentsRT2;
	$row['paymentsRT3'] = $paymentsRT3;
	$row['paymentsRT4'] = $paymentsRT4;
	$row['possiblePaymentsRT1'] = $possiblePaymentsRT1;	
	$row['possiblePaymentsRT2'] = $possiblePaymentsRT2;	
	$row['possiblePaymentsRT3'] = $possiblePaymentsRT3;	
	$row['possiblePaymentsRT4'] = $possiblePaymentsRT4;		
	$row['revenueRT'] = $revenueRT;
	$row['rpsRT'] = $rpsRT;	
	
	// Save this row's cohort definition fields for comparison in the next row
	$lastSource = strtoupper($row["source"]);
	$lastCohort = $row["cohort"];
	$lastPayWeek = $row["payWeek"];
	$lastProduct =  strtoupper($row["product"]);
	
	// Write to output
	//print_r($row);
	if($text) if(! fwrite ($handle, implode($delimeter, $row) . "\n")) throw new Exception("Could not write to output file.");
	if($xml) {
		foreach($row as $key=>$value) {
			$xw->writeElement($key,$value);
		}
		$xw->endElement();
	}
	if($db) {
		foreach($row as $k=>$v) {
			$insert->bindValue(":$k", $v);
		}
		$insert->execute();
	}
}

// Close file
if($xml) $xw->endElement();
if($xml) file_put_contents('./'.$output_filename.'.xml',$xw->outputMemory());
echo "Done...\n";
//echo "$cols \n$vals \n";