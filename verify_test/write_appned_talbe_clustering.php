<?php

require_once  "../vendor/autoload.php";

use Google\Cloud\BigQuery\BigQueryClient;

$location = 'asia-northeast1';
$project = '';
$dataset = 'verify_unison_clustering';
$table = 'access_lily';
$expiration = 1000 * 60 * 60 *  24 * 365;
$keyPath = empty(getenv('GOOGLE_APPLICATION_CREDENTIALS') === false) ? getenv('GOOGLE_APPLICATION_CREDENTIALS') : '';
$writeDisposition = 'WRITE_APPEND';
$priority = 'BATCH';

// クラスタフィールドを取得
$sql_cluster_field = sprintf('
  SELECT column_name,clustering_ordinal_position FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = "%s"
  AND clustering_ordinal_position IS NOT NULL
  ORDER BY clustering_ordinal_position', $project, $dataset, $table);


$sql = sprintf('
SELECT
   _db,
   _table,
   action,
   app_binary_md5,
   app_signature,
   "write_append when 2022.06.17" AS device,
   elapsed,
   env,
   error_message,
   fingerprint,
   http_status_code,
   ip,
   ip_country,
   is_debugger_attached,
   is_device_compromised,
   memory_usage,
   os_version,
   request,
   request_app_version,
   request_asset_hash,
   request_country_code,
   request_debug_time_stamp,
   request_last_signature,
   request_platform,
   request_signature,
   response,
   saved_time,
   server_error_code,
   shared_lib_count,
   tid,
   time,
   trigger_name,
   uid,
   DATE("2022-06-17") AS dt
from `%s.%s.%s` where dt = DATE("2022-06-10") limit 100000', $project, $dataset, $table);

// クラスタ化された分割テーブルへ有効期限を設定して結果を保存
try {
    $bigquery = new BigQueryClient([
        'projectId'   => $project,
        'location'    => $location,
        'keyFilePath' => $keyPath,
    ]);

    $queryJobConfig = $bigquery
        ->query($sql_cluster_field);

    $queryResults = $bigquery->runQuery(
        $queryJobConfig
    );

    $clusteringFields = [];
    foreach ($queryResults as $row) {
        $clusteringFields[] = $row['column_name'];
    }
} catch (\Exception $e) {
    var_export($e->getMessage());
}

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
        ->timePartitioning([
            'type'                     => 'DAY',
            'expirationMs'             => $expiration,
            'field'                    => 'dt',
            'require_partition_filter' => true,
        ])
        ->clustering(
            [
                'fields' => $clusteringFields,
            ]
        )
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
