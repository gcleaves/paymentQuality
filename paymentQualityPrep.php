<?php
ini_set('date.timezone','UTC');
/*
This script prepares payment data for further analysis.  
It calculates running totals.

1. Run SQL scipt to populate helper table:

set @startDate='2015-09-21';
delete from simplemvas.cohort_subs_week_simple WHERE cohort >= @startDate;
insert into simplemvas.cohort_subs_week_simple
select 
    sub.product
	,dsub.year_week_start cohort
	#,t.source
	,'source'
	,count(distinct sub.request_id2) subscribers
from
    simplemvas.dcb_subscriptors sub
        inner join
		general.dates dsub ON date(sub.subStartDate) = dsub.date
			#inner join
		#simplemvas.dcb_transactions t ON sub.request_id2 = t.request_id2
where
	1=1
	#AND sub.product in ('videospremium')
	and sub.status != - 1
	and sub.product !='###TEMP###'
	and sub.subStartDate >= @startDate
group by 
    sub.product
	,dsub.year_week_start
	#,t.source
;

2. ssh root@db2.crazynetworks.net -p23332 -L 3307:localhost:3306

3. run this script

4. update tableua
*/ 


$backFill = 1;
$cohortType = "week";
$xml = 0;
$text = 1;
$textAppend = 0;
$db = 0;
$output_filename = "/Users/gcleaves/Google Drive/src/payment_quality_simple_week";
//$output_filename = "./payment_quality_devel";
$delimeter = ";";
// Import SQL query
$sqlString = file_get_contents("./payment_data_simple_".$cohortType."_no_source.sql");

/**
 * Last Monday is the most recent cohort we will work with
 * @return \DateTime
 */
function getLastMonday() {
    $lastMonday = strtotime('Monday last week');
    $dt = new DateTime('@'.$lastMonday);
    return $dt;
    //"@1215282385"
}

/**
 * 
 * @global int $text
 * @global type $handle
 * @global string $delimeter
 * @param array $row
 * @param array $lastRow
 * @param DateTime $fillTo
 * @throws Exception
 */
function backFillCohort(array $row, array $lastRow, DateTime $fillTo, $sameCohort = false) {
    global $text, $handle, $delimeter, $cohortType;
    
    $paymentsRT1 = null;
    $paymentsRT2 = null;
    $paymentsRT3 = null;
    $paymentsRT4 = null;
    $possiblePaymentsRT1 = null;
    $possiblePaymentsRT2 = null;
    $possiblePaymentsRT3 = null;
    $possiblePaymentsRT4 = null;
    $response = array();
    $newRow = $lastRow;
    $lastPayWeek = new DateTime($lastRow['payWeek']);
    
    $newRow['notes'] = "backfill " . (($sameCohort) ? "same cohort" : "old cohort");
    file_put_contents('php://stderr', "Cohort did not reach ".$fillTo->format('Y-m-d').": {$lastRow['product']} {$lastRow['source']} {$lastRow['cohort']} [{$lastRow['payWeek']}] \n");
    do {
        $newWeek = $lastPayWeek->add(DateInterval::createFromDateString("1 $cohortType"));
        $newRow['payWeek'] = $newWeek->format('Y-m-d');
        $newRow['subscribers'] = 0;
        $newRow['payments'] = 0;
        $newRow['payers'] = 0;
        $newRow['weeks']++;
        $newRow['possiblePayments'] = $newRow['originalSubscribers'] * $newRow['weeks'];
        $newRow['revenue'] = 0;
        $newRow['rps'] = 0;
        $newRow['possiblePaymentsRT'] = $newRow['possiblePayments'];
        // what if we are in the 1st 4 weeks and RTx needs to be filled in?
        
        //$paymentsRT = $row['payments'];
        // need to fix RT1-4
        switch($newRow['weeks']) {
            case 1:
                // We really shouldn't find ourselves in this position, only when there 
                // is funky data with payments before cohort date
                file_put_contents('php://stderr', "BACKFILL: Existing problem cohort {$row['product']} {$row['source']} {$row['cohort']} {$row['payWeek']} {$row['weeks']}\n");
                $paymentPercent1 = $newRow['paymentsRT'] / $newRow['possiblePayments'];
                $newRow['paymentsRT1'] = $newRow['paymentsRT'];
                $newRow['possiblePaymentsRT1'] = $newRow['possiblePayments'];
                $response['paymentsRT1'] = $newRow['paymentsRT'];
                $response['$possiblePaymentsRT1'] = $newRow['possiblePayments'];                    
                
                break;
            case 2:
                $paymentPercent2 = $newRow['paymentsRT'] / $newRow['possiblePayments'];
                $newRow['paymentsRT2'] = $newRow['paymentsRT'];
                $newRow['possiblePaymentsRT2'] = $newRow['possiblePayments'];
                $response['paymentsRT2'] = $newRow['paymentsRT'];
                $response['possiblePaymentsRT2'] = $newRow['possiblePayments'];     
                
                break;
            case 3:
                $paymentPercent3 = $newRow['paymentsRT'] / $newRow['possiblePayments'];
                $newRow['paymentsRT3'] = $newRow['paymentsRT'];
                $newRow['possiblePaymentsRT3'] = $newRow['possiblePayments'];
                $response['paymentsRT3'] = $newRow['paymentsRT'];
                $response['possiblePaymentsRT3'] = $newRow['possiblePayments'];     
                
                break;
            case 4:
                $paymentPercent4 = $newRow['paymentsRT'] / $newRow['possiblePayments'];
                $newRow['paymentsRT4'] = $newRow['paymentsRT'];
                $newRow['possiblePaymentsRT4'] = $newRow['possiblePayments'];			
                $response['paymentsRT4'] = $newRow['paymentsRT'];
                $response['possiblePaymentsRT4'] = $newRow['possiblePayments'];     
                
                break;				
        }
        
        echo "do ".$newWeek->format('Y-m-d')."\n";
        if(''==trim($newRow['source'])) $newRow['source']='unknown';
        if($text) if(! fwrite ($handle, implode($delimeter, $newRow) . "\n")) throw new Exception("Could not write to output file.");
        //print_r($newRow);
    } while ($newWeek < $fillTo);
    
    return $response;
}

echo getLastMonday()->format('Y-m-d')."\n";
//die();
$config = yaml_parse_file(__DIR__ . '/config.dist.yml');
//print_r($config); exit;

$lastMonday = getLastMonday();
$lastMondayYMD = $lastMonday->format("Y-m-d");
$lastRow = array();

echo "Launching...\n";

echo "Connecting to DB...\n";
// Connect to DB for query
//$dbQ = new PDO(getenv('db_cs'), getenv('db_user'), getenv('db_pass'));
$dbQ = new PDO($config['db_cs'], $config['db_user'], $config['db_pass']);
$dbQ->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

if($db) {
    // Connect to DB for write
    $dbW = new PDO($config['db_cs'], $config['db_user'], $config['db_pass']);
    $dbW->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
}

echo "Opening output file...\n";
// Open output file
$openMethod = ($textAppend) ? "a" : "w";
if ($text) {
    if (!$handle = fopen($output_filename . ".txt", $openMethod)) {
        throw new Exception("Could not open output file.");
    }
}

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
echo "Query finished...\n";

// Initialize vars
$r = 0;
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
$headersExtra[] = 'notes';

echo "Looping through results...\n";
// Loop through results
while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
    $r++;
    //echo "Processing row $r ...\n";
    if ($xml) {
        $xw->startElement("row");
    }
    // Write file headers if this is first row
    if(1==$r) {
        $colcount = $stmt->columnCount();
        for($a=0;$a<$colcount;$a++) {
            $meta = $stmt->getColumnMeta($a);
            $headers[] = $meta['name'];
        }

        // Add headers and custom headers
        if($text && !$textAppend) {
            if(! fwrite ($handle, implode($delimeter, array_merge($headers,$headersExtra)) . "\n")) {
                throw new Exception("Could not write header to output file.");
            }
        }

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
        
        if( ($lastRow['weeks']+1) != $row['weeks'] ) {
            //file_put_contents('php://stderr', "Error in same cohort {$row['product']} {$row['source']} {$row['cohort']} {$row['payWeek']} {$row['weeks']}\n");
            $pw = new DateTime($row['payWeek']);
            if ($backFill) {
                $response = backFillCohort($row, $lastRow, $pw->sub(DateInterval::createFromDateString("1 $cohortType")), true);
                foreach($response as $key=>$value) {
                    $$key = $value;
                }
            }

        }
        
        $paymentsRT += $row['payments'];
        $possiblePaymentsRT = $row['possiblePayments'];
        $revenueRT += $row['revenue'];
        $rpsRT += $row['rps'];

        switch($row['weeks']) {
            case 1:
                // We really shouldn't find ourselves in this position, only when there 
                // is funky data with payments before cohort date
                file_put_contents('php://stderr', "Existing problem cohort {$row['product']} {$row['source']} {$row['cohort']} {$row['payWeek']} {$row['weeks']}\n");
                $paymentPercent1 = $paymentsRT / $row['possiblePayments'];
                $paymentsRT1 = $paymentsRT;
                $possiblePaymentsRT1 = $row['possiblePayments'];				
            case 2:
                $paymentPercent2 = $paymentsRT / $row['possiblePayments'];
                $paymentsRT2 = $paymentsRT;
                $possiblePaymentsRT2 = $row['possiblePayments'];
                break;
            case 3:
                $paymentPercent3 = $paymentsRT / $row['possiblePayments'];
                $paymentsRT3 = $paymentsRT;
                $possiblePaymentsRT3 = $row['possiblePayments'];			
                break;
            case 4:
                $paymentPercent4 = $paymentsRT / $row['possiblePayments'];
                $paymentsRT4 = $paymentsRT;
                $possiblePaymentsRT4 = $row['possiblePayments'];			
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
        
        if(($r != 1) && ($lastRow['payWeek'] != $lastMondayYMD)) {
            echo "old cohort\n";
            if ($backFill) {
                backFillCohort($row, $lastRow, $lastMonday);
            }
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
    
    $lastRow = $row;

    // Write to output
    //print_r($row);
    if ($text) {
        if(''==trim($row['source'])) $row['source']='unknown';
        if (!fwrite($handle, implode($delimeter, $row) . "\n")) {
            throw new Exception("Could not write to output file.");
        }
    }
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
if ($xml) {
    $xw->endElement();
}
if ($xml) {
    file_put_contents('./' . $output_filename . '.xml', $xw->outputMemory());
}
echo "Done...\n";
//echo "$cols \n$vals \n";