<?php

namespace My;

use Google\Cloud\BigQuery\QueryJobConfiguration;

class MyQueryJobConfiguration extends QueryJobConfiguration
{
  /**
   * clustering
   * クラスタ化対象フィールドを最大４つまで指定
   *
   * @param  mixed $clusteringFields [field1, field2, field3, field4]
   * @return void
   */
  public function clustering(?array $clusteringFields = []): QueryJobConfiguration
  {
    if (isset($clusteringFields) && empty($clusteringFields) === false) {
      return parent::clustering(
        [
          'fields' => $clusteringFields,
        ]
      );
    }

    return $this;
  }
}
