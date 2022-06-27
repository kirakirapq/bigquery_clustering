<?php

namespace My;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\QueryJobConfiguration;
use \ReflectionClass;

class MyBigQueryClient extends BigQueryClient
{
    private function getLocation(): string
    {
        $reflectionClass = new ReflectionClass($this);
        $property = $reflectionClass->getParentClass()->getProperty('location');
        $property->setAccessible(true);

        return $property->getValue($this);
    }

    private function getProjectId()
    {
        $reflectionClass = new ReflectionClass($this);
        $property = $reflectionClass->getParentClass()->getProperty('projectId');
        $property->setAccessible(true);

        return $property->getValue($this);
    }

    private function getMapper()
    {
        $reflectionClass = new ReflectionClass($this);
        $property = $reflectionClass->getParentClass()->getProperty('mapper');
        $property->setAccessible(true);

        return $property->getValue($this);
    }

    /**
     * query
     *
     * @param  string $query
     * @param  array $options
     * @return QueryJobConfiguration
     */
    public function query($query, array $options = []): MyQueryJobConfiguration
    {
        return (new MyQueryJobConfiguration(
            $this->getMapper(),
            $this->getProjectId(),
            $options,
            $this->getLocation()
        ))->query($query);
    }
}
