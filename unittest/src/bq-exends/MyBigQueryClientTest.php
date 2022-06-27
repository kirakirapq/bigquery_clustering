<?php

namespace My\UnitTest;

require_once __DIR__ . '/../bootstrap.php';

use My\Util;
use PHPUnit\Framework\TestCase;

class UnisonBigQueryClientTest extends TestCase
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

        // テーブル作成
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

        //　データ挿入
        $sql = '
        SELECT
        "id-1" AS id,
        "Taro" AS name,
        22 AS age,
        DATE("2000-01-01") AS birth,
        "male" AS gender,
        "A" AS blood,
        "jp" AS country';

        $queryJobConfig = $bigqueryClient
            ->query($sql)
            ->destinationTable($t1)
            ->schemaUpdateOptions(['ALLOW_FIELD_ADDITION'])
            ->writeDisposition('WRITE_APPEND')
            ->priority('BATCH');
        $queryResults = $bigqueryClient->runQuery(
            $queryJobConfig,
            ['maxResults' => 0]
        );
    }

    public static function tearDownAfterClass(): void
    {
        $bigqueryClient = Util::getBigQueryClient(self::PROJECT_ID);
        $dataset = $bigqueryClient->dataset(self::DATASET_ID, self::PROJECT_ID);
        $t1 = $dataset->table(self::CLUSTERING_TABLE);

        if ($t1->exists()) {
            $t1->delete();
        }
    }

    /**
     * query
     * @test
     *
     * @return void
     */
    public function query(): void
    {
        $bigqueryClient = Util::getBigQueryClient(self::PROJECT_ID);
        $sql = sprintf('SELECT * FROM `%s.%s`', self::DATASET_ID, self::CLUSTERING_TABLE);

        $queryJobConfig = $bigqueryClient
            ->query($sql)
            ->priority('BATCH');

        $queryResults = $bigqueryClient->runQuery(
            $queryJobConfig,
            ['maxResults' => 1]
        );

        $expected = [
            'id' => 'id-1',
            'name' => 'Taro',
            'age' => 22,
            'birth' => '2000-01-01',
            'gender' => 'male',
            'blood' => 'A',
            'country' => 'jp',
        ];

        foreach ($queryResults as $actual) {
            $this->assertEquals($expected['id'], $actual['id']);
            $this->assertEquals($expected['name'], $actual['name']);
            $this->assertEquals($expected['age'], $actual['age']);
            $this->assertEquals($expected['birth'], $actual['birth']);
            $this->assertEquals($expected['gender'], $actual['gender']);
            $this->assertEquals($expected['country'], $actual['country']);
        }
    }

    /**
     * clustering
     * @test
     * @dataProvider clusteringData
     *
     * @return void
     */
    public function clustering($table, $insert, $select, $expected): void
    {
        $bigqueryClient = Util::getBigQueryClient(self::PROJECT_ID);
        $destinationTable = $bigqueryClient->dataset(self::DATASET_ID)->table($table);
        $clusteringFields = Util::getClusteringFields($bigqueryClient, self::PROJECT_ID, self::DATASET_ID, $table);

        // データを挿入
        $queryJobConfig = $bigqueryClient
            ->query($insert)
            ->destinationTable($destinationTable)
            ->schemaUpdateOptions(['ALLOW_FIELD_ADDITION'])
            ->writeDisposition('WRITE_APPEND')
            ->timePartitioning([
                'type'                     => 'DAY',
                'field'                    => 'birth',
            ])
            ->clustering($clusteringFields)
            ->priority('BATCH');

        $bigqueryClient->runQuery(
            $queryJobConfig,
            ['maxResults' => 1]
        );

        // データ取得して確認
        $selectSQL = sprintf($select, self::DATASET_ID, $table);
        $queryJobConfig = $bigqueryClient
            ->query($selectSQL)
            ->priority('BATCH');

        $queryResults = $bigqueryClient->runQuery(
            $queryJobConfig,
            ['maxResults' => 1]
        );

        foreach ($queryResults as $actual) {
            $this->assertEquals($expected['id'], $actual['id']);
            $this->assertEquals($expected['name'], $actual['name']);
            $this->assertEquals($expected['age'], $actual['age']);
            $this->assertEquals($expected['birth'], $actual['birth']);
            $this->assertEquals($expected['gender'], $actual['gender']);
            $this->assertEquals($expected['country'], $actual['country']);
        }
    }

    public function clusteringData(): array
    {
        $insert = '
        SELECT
        "id-2" AS id,
        "Hanako" AS name,
        20 AS age,
        DATE("2002-01-01") AS birth,
        "female" AS gender,
        "AB" AS blood,
        "jp" AS country';

        $expected = [
            'id' => 'id-2',
            'name' => 'Hanako',
            'age' => 20,
            'birth' => '2002-01-01',
            'gender' => 'female',
            'blood' => 'AB',
            'country' => 'jp',
        ];

        return [
            'clustering table case' => [
                'table' => self::CLUSTERING_TABLE,
                'insert_sql' => $insert,
                'select_sql' => 'SELECT * FROM `%s.%s` WHERE birth="2002-01-01"',
                'expected' => $expected,
            ],
            'not clustering table case' => [
                'table' => self::UNCLUSTERING_TABLE,
                'insert_sql' => $insert,
                'select_sql' => 'SELECT * FROM `%s.%s` WHERE birth="2002-01-01"',
                'expected' => $expected,
            ],
        ];
    }
}
