## 概要

|ファイル|概要|
|:--:|:--|
|copy_table|テーブルコピーを作成|
|bq_data_migration.php|クラスタテーブルのデータ移行|
|update_talbe_clustering.php|テーブルをクラスタ化|

## オプション
### copy_table
### 概要
* オプションで指定したテーブルのコピーを作成します
* コピーテーブル先：{project_id}.unison_clustering_temp_tables.{dataset_id}_{table_id}_copy

|オプション|説明|
|:--:|:--|
|-p|プロジェクトID|
|-d|データセットID|
|-t|テーブルID|

### サンプル

```
 % php copy_table.php -p gree-wfs-dev -d gareth_test -t access
[gareth_test.access] Copy Job started.
[gareth_test.access] Copy Job finished.
[s.gareth_test] total time: access

src % bq ls gree-wfs-dev:unison_clustering_temp_tables           
          tableId           Type    Labels               Time Partitioning                        Clustered Fields          
 ------------------------- ------- -------- -------------------------------------------- ---------------------------------- 
  gareth_test_access_copy   TABLE            DAY (field: dt, expirationMs: 31536000000)   uid, fingerprint, os_version, ip  
```

### bq_data_migration
### 概要
* オプションで指定したテーブルへデータ移行を実施
* 移行元テーブル：{project_id}.unison_clustering_temp_tables.{dataset_id}_{table_id}_copy

|オプション|説明|
|:--:|:--|
|-p|プロジェクトID|
|-d|データセットID|
|-t|テーブルID|
|-s|dtカラムの開始日|
|-d|dtカラムの終了日|

### サンプル

```
src % php bq_data_migration.php -p gree-wfs-dev -d gareth_test -t access -s=2021-06-24 -e=2021-06-24
Date from 2021-06-24 to 2021-06-24.
[2021-06-24] Job started.
[2021-06-24] Job finished.
[2021-06-24] Lap time: 4 sec, total time: 4
All job finished.
```

### update_talbe_clustering
### 概要
* オプションで指定したテーブルをクラスタリング
* 移行元テーブル：{project_id}.unison_clustering_temp_tables.{dataset_id}_{table_id}_copy

|オプション|説明|
|:--:|:--|
|-c|クラスタ化するフィールドをリストで指定|
|-p|プロジェクトID|
|-d|データセットID|
|-t|テーブルID|

### サンプル

```
src % php update_talbe_clustering.php -c=uid,master_chapter_id,master_quest_id,event_id -p=gree-tempest -d=tempest -t=quest
src % bq show  --format prettyjson gree-tempest:tempest.quest | jq '{id,clustering}'
{
  "id": "gree-tempest:tempest.quest",
  "clustering": {
    "fields": [
      "uid",
      "master_chapter_id",
      "master_quest_id",
      "event_id"
    ]
  }
}
```
