<?php

namespace My;

use Google\Cloud\BigQuery\BigQueryClient;
use \Exception;

class Util
{
    public static function getBigQueryClient(string $projectId): BigQueryClient
    {
        return new MyBigQueryClient([
            'projectId'   => $projectId,
            'location'    => 'asia-northeast1',
            'keyFilePath' => ENV_SERVICE_ACCOUNT_KEY_FILE,
        ]);
    }

    public static function getClusteringFields(BigQueryClient $bigQueryClient, string $projectId, string $datasetId, string $tableId): array
    {
        $templete = '
        SELECT column_name,clustering_ordinal_position FROM `{projectId}.{datasetId}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = "{tableId}"
        AND clustering_ordinal_position IS NOT NULL
        ORDER BY clustering_ordinal_position';

        $replaceFrom = [
            '/{projectId}/',
            '/{datasetId}/',
            '/{tableId}/',
        ];
        $replaceTo = [
            $projectId,
            $datasetId,
            $tableId,
        ];

        $sql = preg_replace(
            $replaceFrom,
            $replaceTo,
            $templete
        );

        $retry = 0;
        while (true) {
            try {
                $queryJobConfig = $bigQueryClient->query($sql);
                $queryResults = $bigQueryClient->runQuery($queryJobConfig);

                $clusteringFields = [];
                foreach ($queryResults as $row) {
                    $clusteringFields[] = $row['column_name'];
                }

                return $clusteringFields;
            } catch (Exception $e) {
                if ($retry >= 3) {
                    throw $e;
                }
                // $log = new Logger();
                // $log->notice(
                //     "exception was thrown in the BigQuery of Util.getClusteringFields request. message:[{$e->getMessage()}] retry:[{$retry}], " .
                //         "project_id:[{$projectId}], " .
                //         "dataset_id:[{$datasetId}], " .
                //         "table_id:[{$tableId}], "
                // );
                //$baseSecの半分(2.5sec)ずつ増加
                $sleep = 0;
                $baseSec = 5 * 1000000;
                $sleep = $sleep === 0 ? $baseSec : $sleep + ($baseSec / 2);
                sleep($sleep);
                $retry++;
                continue;
            }
        }
    }
}
