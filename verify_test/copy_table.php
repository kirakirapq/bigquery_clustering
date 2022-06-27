<?php

require_once  "../vendor/autoload.php";

use Google\Cloud\BigQuery\BigQueryClient;

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

$expiration = 1000 * 60 * 60 *  24 * 100;
$keyPath = empty(getenv('GOOGLE_APPLICATION_CREDENTIALS') === false) ? getenv('GOOGLE_APPLICATION_CREDENTIALS') : '';

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
      "datasetId" => "unison_clustering_temp_tables",
      "tableId" => sprintf('%s_%s_copy', $dataset, $table),
    ],
    "createDisposition" => "CREATE_IF_NEEDED",
    "writeDisposition" => "WRITE_TRUNCATE",
  ];

  $copyJobConfig = $bigQuery->copy($options);
  $job = $bigquery->runJob($copyJobConfig);

  $currentTime = time();
  print_r(sprintf('[%s.%s] Copy Job finished.', $dataset, $table) . PHP_EOL);
  print_r(sprintf('[s.%s] total time: %s', $dataset, $table, $currentTime - $startTime) . PHP_EOL);
} catch (\Exception $e) {
  var_export($e->getMessage());
}
