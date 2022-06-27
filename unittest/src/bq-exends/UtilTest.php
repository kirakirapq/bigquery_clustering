<?php

namespace My\UnitTest;

require_once __DIR__ . '/../bootstrap.php';

use Unison\Util;
use PHPUnit\Framework\TestCase;
use Google\Cloud\BigQuery\BigQueryClient;

class UtilTest extends TestCase
{
    const PROJECT_ID = 'my-project';
    const DATASET_ID = 'verify_clustering';
    const CLUSTERING_TABLE = 'utiltest_clustering';
    const UNCLUSTERING_TABLE = 'utiltest_unclustering';
    const TABLE_SCHEMA =  [
        'fields' => [
            [
                'name' => 'id',
                'type' => 'STRING',
                'mode' => 'NULLABLE'
            ],
            [
                'name' => 'name',
                'type' => 'STRING',
                'mode' => 'NULLABLE'
            ],
            [
                'name' => 'age',
                'type' => 'INT64',
                'mode' => 'NULLABLE'
            ],
            [
                'name' => 'birth',
                'type' => 'DATE',
                'mode' => 'NULLABLE'
            ],
            [
                'name' => 'gender',
                'type' => 'STRING',
                'mode' => 'NULLABLE'
            ],
            [
                'name' => 'blood',
                'type' => 'STRING',
                'mode' => 'NULLABLE'
            ],
            [
                'name' => 'country',
                'type' => 'STRING',
                'mode' => 'NULLABLE'
            ],
        ]
    ];
    const CLUSTERING_FIELD = [
        'fields' => [
            'name', 'age', 'gender', 'country',
        ]
    ];
    const TIME_PARTITION = [
        'type' => 'DAY',
        'field' => 'birth',
    ];

    public static function setUpBeforeClass(): void
    {
        $bigqueryClient = Util::getBigQueryClient(self::PROJECT_ID);
        $dataset = $bigqueryClient->dataset(self::DATASET_ID, self::PROJECT_ID);

        if ($dataset->exists() === false) {
            $bigqueryClient->createDataset(self::DATASET_ID);
        }

        $t1 = $dataset->table(self::CLUSTERING_TABLE);
        $t2 = $dataset->table(self::UNCLUSTERING_TABLE);

        if ($t1->exists() === false) {
            $option =
                [
                    'schema' => self::TABLE_SCHEMA,
                    'clustering' => self::CLUSTERING_FIELD,
                    'timePartitioning' => self::TIME_PARTITION,
                ];
            $dataset->createTable(self::CLUSTERING_TABLE, $option);
        }

        if ($t2->exists() === false) {
            $option =
                [
                    'schema' => self::TABLE_SCHEMA,
                    'timePartitioning' => self::TIME_PARTITION,
                ];
            $dataset->createTable(self::UNCLUSTERING_TABLE, $option);
        }
    }

    public static function tearDownAfterClass(): void
    {
        $bigqueryClient = Util::getBigQueryClient(self::PROJECT_ID);
        $dataset = $bigqueryClient->dataset(self::DATASET_ID, self::PROJECT_ID);
        $t1 = $dataset->table(self::CLUSTERING_TABLE);
        $t2 = $dataset->table(self::UNCLUSTERING_TABLE);

        if ($t1->exists()) {
            $t1->delete();
        }

        if ($t2->exists()) {
            $t2->delete();
        }
    }

    /**
     * getBigQueryClient
     * @test
     *
     * @return void
     */
    public function getBigQueryClient(): void
    {
        $bigqueryClient = Util::getBigQueryClient(self::PROJECT_ID);

        $this->assertInstanceOf(BigQueryClient::class, $bigqueryClient);
    }

    /**
     * getClusteringFields
     * @test
     * @dataProvider getClusteringFieldsData
     *
     * @return void
     */
    public function getClusteringFields($tableId, $expected): void
    {
        $bigqueryClient = Util::getBigQueryClient(self::PROJECT_ID);
        $clusteringFields = Util::getClusteringFields($bigqueryClient, self::PROJECT_ID, self::DATASET_ID, $tableId);

        $this->assertEquals($expected, $clusteringFields);
    }

    public function getClusteringFieldsData(): array
    {
        return [
            'clustering table' => [
                'table_id' => self::CLUSTERING_TABLE,
                'expected' => self::CLUSTERING_FIELD['fields'],
            ],
            'not clustering table' => [
                'table_id' => self::UNCLUSTERING_TABLE,
                'expected' => [],
            ],
        ];
    }
}
