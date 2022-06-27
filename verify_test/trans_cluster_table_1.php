<?php

require_once  "../vendor/autoload.php";

use Google\Cloud\BigQuery\BigQueryClient;

$location = 'asia-northeast1';
$project = '';
$dataset = 'verify_unison_clustering';
$table = 'clusterd_original_table';
$expiration = 1000 * 60 * 60 *  24 * 100;
$keyPath = empty(getenv('GOOGLE_APPLICATION_CREDENTIALS') === false) ? getenv('GOOGLE_APPLICATION_CREDENTIALS') : '';
$writeDisposition = 'WRITE_APPEND';
$priority = 'INTERACTIVE';

$sql = sprintf('
select
   (id + 10000) as id,
   name,
   name_kana,
   name_roma,
   sex,
   tel,
   mobile,
   email,
   postal_code,
   pref,
   city,
   town,
   address1,
   address2,
   birth_day,
   age,
   birth_place,
   blood,
   rund,
   password
from `%s.%s.original_table` where id <= 100', $project, $dataset);

// クラスタ化されたテーブルへ結果を保存（分割テーブルでも可）
try {

  $bigquery = new BigQueryClient([
    'projectId'   => $project,
    'location'    => $location,
    'keyFilePath' => $keyPath,
  ]);
  $destinationTable = (new BigQueryClient(['projectId' => $project]))
    ->dataset($dataset)
    ->table($table);

  // timePartitioning　は必須ではない
  $queryJobConfig = $bigquery
    ->query($sql)
    ->destinationTable($destinationTable)
    ->schemaUpdateOptions(['ALLOW_FIELD_ADDITION'])
    ->writeDisposition($writeDisposition)
    ->priority($priority);

  $queryResults = $bigquery->runQuery(
    $queryJobConfig,
    ['maxResults' => 0]
  );

  // var_export($queryJobConfig);
} catch (\Exception $e) {
  var_export($e->getMessage());
}
