<?php

require_once  "../vendor/autoload.php";

use Google\Cloud\BigQuery\BigQueryClient;

$location = 'asia-northeast1';
$project = '';
$dataset = 'verify_unison_clustering';
$table = 'clusterd_original_table';
$expiration = 1000 * 60 * 60 *  24 * 100;
$keyPath = empty(getenv('GOOGLE_APPLICATION_CREDENTIALS') === false) ? getenv('GOOGLE_APPLICATION_CREDENTIALS') : '';

// クラスタ化された分割テーブルの有効期限を更新
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
      'timePartitioning' => [
        'expirationMs' => $expiration,
      ],
    ]);
} catch (\Exception $e) {
  var_export($e->getMessage());
}
