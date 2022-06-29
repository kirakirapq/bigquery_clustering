<?php

require_once  "../vendor/autoload.php";

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\ExponentialBackoff;

$longopts  = array(
  "project_id:",
  "dataset:",
  "table:",
);

$short_options = 'p:t:d:';
$options = getopt($short_options, $longopts);


$location = 'asia-northeast1';
$project = $options['project_id'] ?? ($options['p'] ?? '');
$dataset = $options['dataset'] ?? ($options['d'] ?? '');
$table   = $options['table'] ?? ($options['t'] ?? '');
$destDataset = "unison_clustering_temp_tables"; //固定値
$destTable = sprintf('%s_%s_copy', $dataset, $table);

$expiration = 1000 * 60 * 60 *  24 * 100;
$keyPath = empty(getenv('GOOGLE_APPLICATION_CREDENTIALS') === false) ? getenv('GOOGLE_APPLICATION_CREDENTIALS') : '';



// JobConfigurationTableCopyクラス
// {
//   "sourceTable": {
//     object (TableReference)
//   },
//   "sourceTables": [
//     {
//       object (TableReference)
//     }
//   ],
//   "destinationTable": {
//     object (TableReference)
//   },
//   "createDisposition": string,
//   "writeDisposition": string,
//   "destinationEncryptionConfiguration": {
//     object (EncryptionConfiguration)
//   },
//   "operationType": enum (OperationType),
//   "destinationExpirationTime": string
// }
//  $copyJobConfig = $bigQuery->copy()
// ->sourceTable($otherTable)
// ->destinationTable($myTable);

// テーブルをクラスタ化
try {
  $startTime = time();
  print_r(sprintf('[%s.%s] Copy Job started.', $dataset, $table) . PHP_EOL);
  $bigquery = new BigQueryClient([
    'projectId'   => $project,
    'location'    => $location,
    'keyFilePath' => $keyPath,
  ]);


  $options = [
    "sourceTable" => [
      "projectId" => $project,
      "datasetId" => $dataset,
      "tableId" => $table,
    ],
    "destinationTable" => [
      "projectId" => $project,
      "datasetId" => $destDataset,
      "tableId" => sprintf('%s_%s_copy', $dataset, $table),
    ],
    "createDisposition" => "CREATE_IF_NEEDED",
    "writeDisposition" => "WRITE_TRUNCATE",
  ];

  // sorce table
  $sorceDatasetId = $bigquery->dataset($dataset);
  $sourceTable = $sorceDatasetId->table($table);
  // dest table
  $destDatasetId = $bigquery->dataset($destDataset);
  if ($destDatasetId->exists() === false) {
    $bigquery->createDataset($destDataset);
  }
  $destinationTable = $destDatasetId->table($destTable);


  $copyJobConfig = $bigquery->copy()
    ->sourceTable($sourceTable)
    ->destinationTable($destinationTable);
  $job = $bigquery->runJob($copyJobConfig);


  $currentTime = time();
  print_r(sprintf('[%s.%s] Copy Job finished.', $dataset, $table) . PHP_EOL);
  print_r(sprintf('[%s.%s] total time: %s', $dataset, $table, $currentTime - $startTime) . PHP_EOL);
} catch (\Exception $e) {
  var_export($e->getMessage());
}
