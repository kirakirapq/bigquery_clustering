<?php

require_once  "../vendor/autoload.php";

use Google\Cloud\BigQuery\BigQueryClient;
use Carbon\Carbon;

ini_set("memory_limit", -1);

$longopts  = array(
    "project_id:",
    "dataset:",
    "from_dataset_table:",
    "to_table:",
    "start:",
    "end:",
);

$short_options = 'p:d:f:t:s:e:';
$options = getopt($short_options, $longopts);

$location = 'asia-northeast1';
$project = $options['project_id'] ?? ($options['p'] ?? '');
$colmuns = explode(',', $options['colmuns'] ?? ($options['c'] ?? ''));
$dataset = $options['dataset'] ?? ($options['d'] ?? 'verify_unison_clustering');
$tempTable   = $options['from_dataset_table'] ?? ($options['f'] ?? '');
$destTable   = $options['to_table'] ?? ($options['t'] ?? '');
$start = $options['start'] ?? ($options['s'] ?? ''); // 1番古いデータ
$end = $options['end'] ?? ($options['e'] ?? ''); // テスト開始日

$expiration = 1000 * 60 * 60 *  24 * 365;
$keyPath = empty(getenv('GOOGLE_APPLICATION_CREDENTIALS') === false) ? getenv('GOOGLE_APPLICATION_CREDENTIALS') : '';
$writeDisposition = 'WRITE_TRUNCATE';
$priority = 'BATCH';


// SELECT dt FROM `gree-unison-dev.verify_unison_clustering.access_lily_copy` WHERE dt <= "2021-12-31" group by dt order by dt asc

$endDate = new Carbon($end); // テスト終了日
$currendDate = new Carbon($start);
$errors = []; // 失敗データを管理

// 開始時間
$startTime = $prevTime = time();
$lapTimes = [];
print_r(sprintf('Date from %s to %s.', $currendDate->format('Y-m-d'), $endDate->format('Y-m-d')) . PHP_EOL);

$bigquery = new BigQueryClient([
    'projectId'   => $project,
    'location'    => $location,
    'keyFilePath' => $keyPath,
]);

try {
    // クラスタフィールドを取得
    $sql_cluster_field = sprintf('
        SELECT column_name,clustering_ordinal_position FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = "%s"
        AND clustering_ordinal_position IS NOT NULL
        ORDER BY clustering_ordinal_position', $project, $dataset, $destTable);

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

while ($currendDate <= $endDate) {
    print_r(sprintf('[%s] Job started.', $currendDate->format('Y-m-d')) . PHP_EOL);
    // クラスタ化された分割テーブルへ有効期限を設定して結果を保存
    try {
        // 対象日付を取得
        $date = $currendDate->format('Y-m-d');
        // テーブルを取得
        $tableWithPartition = sprintf('%s$%s', $destTable, str_replace('-', '', $date));
        // SQLを生成
        $sql = sprintf('
            SELECT
            *
            FROM `%s`
            WHERE dt = DATE("%s")', $tempTable, $date);

        $destinationTable = (new BigQueryClient(['projectId' => $project]))
            ->dataset($dataset)
            ->table($tableWithPartition);

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

        $currentTime = time();
        $lapTimes[] = [
            'target_date' => $date,
            'start_time' => $startTime,
            'preview_time' => $prevTime,
            'current_time' => $currentTime,
            'lap_time' => $currentTime - $prevTime,
            'total_time' => $currentTime - $startTime,
        ];
        print_r(sprintf('[%s] Job finished.', $date) . PHP_EOL);
        print_r(sprintf('[%s] Lap time: %s sec, total time: %s', $date, $currentTime - $prevTime, $currentTime - $startTime) . PHP_EOL);
        $prevTime = $currentTime;
    } catch (\Exception $e) {
        $errors[] =
            [
                'target_date' => $currendDate->format('Y-m-d'),
                'message' => $e->getMessage(),
            ];
    }
    // 対象日を加算
    $currendDate->addDays(1);
}
print_r('All job finished.' . PHP_EOL);

if (empty($errors) == false) {
    print_r('Has error.' . PHP_EOL);
    foreach ($errors as $error) {
        print_r(sprintf('[%s] Message: %s',  $error['target_date'], $error['message']) . PHP_EOL);
    }
}

// print_r('processing time.' . PHP_EOL);
// foreach ($lapTimes as $laptime) {
    // print_r(sprintf('[%s] Lap time: %s sec', $laptime['target_date'], $laptime['lap_time']) . PHP_EOL);
// }
// print_r('job finished.' . PHP_EOL);
