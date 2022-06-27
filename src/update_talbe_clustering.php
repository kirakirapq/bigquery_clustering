<?php

require_once  "../vendor/autoload.php";

use Google\Cloud\BigQuery\BigQueryClient;

$longopts  = array(
  "colmuns:",
  "project_id:",
  "dataset:",
  "table:",
);

$short_options = 'p:c:t:d::';
$options = getopt($short_options, $longopts);


$location = 'asia-northeast1';
$project = $options['project_id'] ?? ($options['p'] ?? 'gree-unison-dev');
$colmuns = explode(',', $options['colmuns'] ?? ($options['c'] ?? ''));
$dataset = $options['dataset'] ?? ($options['d'] ?? 'verify_unison_clustering');
$table   = $options['table'] ?? ($options['t'] ?? '');

$expiration = 1000 * 60 * 60 *  24 * 100;
$keyPath = empty(getenv('GOOGLE_APPLICATION_CREDENTIALS') === false) ? getenv('GOOGLE_APPLICATION_CREDENTIALS') : '';

// テーブルをクラスタ化
try {
  $bigquery = new BigQueryClient([
    'projectId'   => $project,
    'location'    => $location,
    'keyFilePath' => $keyPath,
  ]);


  // timePartitioning = nullで無期限（0)
  // expirationTimeは変更できない？
  $bigquery->dataset($dataset)
    ->table($table)
    ->update([
      'clustering' => [
        'fields' => $colmuns
      ],
    ]);
} catch (\Exception $e) {
  var_export($e->getMessage());
}
