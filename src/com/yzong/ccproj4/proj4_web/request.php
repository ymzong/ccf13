<?php
if (!isset($_REQUEST['term']))
  {
    echo 'error';
    exit;
  }

include('./httpful.phar');

$tableName = "CorpusTable";
$searchTerm = strtolower(trim($_REQUEST['term']));

// Connect to HBASE server
$url = "http://localhost:8080/" . $tableName . "/" . urlencode($searchTerm);
$response = \Httpful\Request::get($url)->addHeader('Accept', 'application/json')->send();

// iterate through response, adding each to array
$data = array();
$json = json_decode($response, true);
$row  = $json["Row"];
$Cell = $row["Cell"];

foreach ($Cell as $item)
  {
    $column = base64_decode($item["@column"]);
    if (substr($column, -4) === "freq")
      {
        continue;
      }

    $url             = "http://localhost:8080/" . $tableName . "/" . urlencode($searchTerm) . "/" . $column;
    $cellResponse    = \Httpful\Request::get($url)->addHeader('Accept', 'application/json')->send();
    $decodedResponse = json_decode($cellResponse, true);
    $word            = base64_decode($decodedResponse["Row"]["Cell"]["$"]);
    
    $data[] = array(
        'label' => $word,
        'value' => $searchTerm . " " . $word
    );
  }

// return JSON encoded array to the JQuery autocomplete function
echo json_encode($data);
flush();
?>

