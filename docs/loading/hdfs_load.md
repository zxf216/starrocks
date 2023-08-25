# HDFSからデータをロードする

import InsertPrivNote from '../assets/commonMarkdown/insertPrivNote.md'

StarRocksは、[Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md)を使用してHDFSから大量のデータをStarRocksにロードすることができます。

Broker Loadは非同期ロードモードで実行されます。ロードジョブを送信すると、StarRocksは非同期でジョブを実行します。ジョブの結果をクエリするには、`SELECT * FROM information_schema.loads`を使用できます。この機能はv3.1以降でサポートされています。詳細については、このトピックの"[ロードジョブの表示](#view-a-load-job)"セクションを参照してください。

Broker Loadは、複数のデータファイルをロードするために実行される各ロードジョブのトランザクション的な原子性を保証します。つまり、1つのロードジョブで複数のデータファイルのロードがすべて成功するか、すべて失敗するかのいずれかです。一部のデータファイルのロードが成功し、他のファイルのロードが失敗することはありません。

さらに、Broker Loadはデータロード時のデータ変換をサポートし、データロード中にUPSERTおよびDELETE操作によるデータの変更をサポートします。詳細については、[ロード時のデータ変換](../loading/Etl_in_loading.md)および[ロードを介したデータの変更](../loading/Load_to_Primary_Key_tables.md)を参照してください。

<InsertPrivNote />

## 背景情報

v2.4以前では、StarRocksはBroker Loadジョブを実行する際に、ブローカーを使用してStarRocksクラスタと外部ストレージシステムの間の接続を設定していました。そのため、ロードステートメントで使用するブローカーを指定するために`WITH BROKER "<broker_name>"`を入力する必要があります。これは「ブローカーベースのロード」と呼ばれます。ブローカーは、ファイルシステムインターフェースと統合された独立したステートレスサービスです。ブローカーを使用することで、StarRocksは外部ストレージシステムに保存されているデータファイルにアクセスして読み取ることができ、これらのデータファイルのデータを事前処理してロードするために独自の計算リソースを使用することができます。

v2.5以降、StarRocksはBroker Loadジョブを実行する際に、ブローカーを使用してStarRocksクラスタと外部ストレージシステムの間の接続を設定する必要がありません。そのため、ロードステートメントでブローカーを指定する必要はありませんが、`WITH BROKER`キーワードは引き続き保持する必要があります。これは「ブローカーフリーローディング」と呼ばれます。

HDFSにデータが保存されている場合、ブローカーフリーローディングが機能しない場合があります。これは、データが複数のHDFSクラスタにまたがって保存されている場合や、複数のKerberosユーザが設定されている場合に発生する可能性があります。このような場合、代わりにブローカーベースのローディングを使用することができます。これを成功させるためには、少なくとも1つの独立したブローカーグループが展開されていることを確認してください。これらの状況で認証構成とHA構成を指定する方法については、[HDFS](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md#hdfs)を参照してください。

> **注意**
>
> [SHOW BROKER](../sql-reference/sql-statements/Administration/SHOW%20BROKER.md)ステートメントを使用して、StarRocksクラスタに展開されているブローカーを確認できます。ブローカーが展開されていない場合は、[ブローカーの展開](../deployment/deploy_broker.md)に記載されている手順に従ってブローカーを展開できます。

## サポートされているデータファイル形式

ブローカーロードは、次のデータファイル形式をサポートしています。

- CSV

- Parquet

- ORC

> **注意**
>
> CSVデータの場合、次の点に注意してください：
>
> - テキスト区切り文字として、50バイトを超えないUTF-8文字列（例：カンマ（,）、タブ、パイプ（|））を使用できます。
> - Null値は`\N`を使用して示されます。たとえば、データファイルは3つの列から構成されており、そのデータファイルのレコードは最初と3番目の列にデータを保持していますが、2番目の列にはデータがありません。この場合、2番目の列にはNull値を示すために`\N`を使用する必要があります。つまり、レコードは`a,\N,b`としてコンパイルする必要があります。`a,,b`は、レコードの2番目の列に空の文字列があることを示します。

## 動作原理

FEにロードジョブを送信すると、FEはクエリプランを生成し、利用可能なBEの数とロードするデータファイルのサイズに基づいてクエリプランを分割し、各クエリプランの部分を利用可能なBEに割り当てます。ロード中、関連する各BEは外部ストレージシステムからデータファイルのデータを取得し、データを前処理してStarRocksクラスタにデータをロードします。すべてのBEがクエリプランの各部分を完了した後、FEはロードジョブが成功したかどうかを判断します。

以下の図は、Broker Load ジョブのワークフローを示しています。

![Broker Load のワークフロー](../assets/broker_load_how-to-work_en.png)

## データの準備例

1. HDFS クラスタにログインし、指定されたパス（例：`/user/starrocks/`）に、2 つの CSV 形式のデータファイル `file1.csv` と `file2.csv` を作成します。両方のファイルは、ユーザー ID、ユーザー名、ユーザースコアの 3 つの列で構成されています。

   - `file1.csv`

```
     1,Lily,21
     2,Rose,22
     3,Alice,23
     4,Julia,24
```

   - `file2.csv`

```
     5,Tony,25
     6,Adam,26
     7,Allen,27
     8,Jacky,28
```

2. StarRocks データベース（例：`test_db`）にログインし、2 つのプライマリキー テーブル `table1` と `table2` を作成します。両方のテーブルは、`id`、`name`、`score` の 3 つの列で構成されており、`id` がプライマリキーです。

```
   CREATE TABLE `table1`
      (
          `id` int(11) NOT NULL COMMENT "user ID",
          `name` varchar(65533) NULL DEFAULT "" COMMENT "user name",
          `score` int(11) NOT NULL DEFAULT "0" COMMENT "user score"
      )
          ENGINE=OLAP
          PRIMARY KEY(`id`)
          DISTRIBUTED BY HASH(`id`);
             
   CREATE TABLE `table2`
      (
          `id` int(11) NOT NULL COMMENT "用户 ID",
          `name` varchar(65533) NULL DEFAULT "" COMMENT "user name",
          `score` int(11) NOT NULL DEFAULT "0" COMMENT "user score"
      )
          ENGINE=OLAP
          PRIMARY KEY(`id`)
          DISTRIBUTED BY HASH(`id`);
```

## ロードジョブの作成

以下の例では、CSV 形式とシンプルな認証方法を使用しています。他の形式でデータをロードする方法、HA 構成を指定する方法、および Kerberos 認証方法を使用する場合に設定する必要のある認証パラメータについては、[BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md) を参照してください。

### 単一のデータファイルを単一のテーブルにロードする

#### 例

次のステートメントを実行して、`file1.csv` のデータを `table1` にロードします。

```
LOAD LABEL test_db.label_brokerload_singlefile_singletable
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/file1.csv")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

#### データのクエリ

ロードジョブを送信した後、`SELECT * FROM information_schema.loads` を使用してロードジョブの結果を表示できます。この機能は v3.1 以降でサポートされています。詳細については、このトピックの "[ロードジョブの表示](#view-a-load-job)" セクションを参照してください。

ロードジョブが成功したことを確認した後、[SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md) を使用して `table1` のデータをクエリできます。

```
SELECT * FROM table1;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|    1 | Lily  |    21 |
|    2 | Rose  |    22 |
|    3 | Alice |    23 |
|    4 | Julia |    24 |
+------+-------+-------+
4 rows in set (0.01 sec)
```

### 複数のデータファイルを単一のテーブルにロードする

#### 例

次のステートメントを実行して、HDFS クラスタの `/user/starrocks/` パスに格納されているすべてのデータファイル (`file1.csv` と `file2.csv`) を `table1` にロードします。

```
LOAD LABEL test_db.label_brokerload_allfile_singletable
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/*")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

#### データのクエリ

ロードジョブを送信した後、`SELECT * FROM information_schema.loads` を使用してロードジョブの結果を表示できます。この機能はv3.1以降でサポートされています。詳細については、このトピックの"[ロードジョブの表示](#ロードジョブの表示)"セクションを参照してください。

ロードジョブが成功したことを確認した後、`table1` のデータをクエリするために [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md) を使用できます:

```
SELECT * FROM table1;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|    1 | Lily  |    21 |
|    2 | Rose  |    22 |
|    3 | Alice |    23 |
|    4 | Julia |    24 |
|    5 | Tony  |    25 |
|    6 | Adam  |    26 |
|    7 | Allen |    27 |
|    8 | Jacky |    28 |
+------+-------+-------+
4 rows in set (0.01 sec)
```

### 複数のデータファイルを複数のテーブルにロードする

#### 例

次のステートメントを実行して、`file1.csv` と `file2.csv` のデータをそれぞれ `table1` と `table2` にロードします:

```
LOAD LABEL test_db.label_brokerload_multiplefile_multipletable
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/file1.csv")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
    ,
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/file2.csv")
    INTO TABLE table2
    COLUMNS TERMINATED BY ","
    (id, name, score)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

#### データのクエリ

ロードジョブを送信した後、`SELECT * FROM information_schema.loads` を使用してロードジョブの結果を表示できます。この機能はv3.1以降でサポートされています。詳細については、このトピックの"[ロードジョブの表示](#ロードジョブの表示)"セクションを参照してください。

ロードジョブが成功したことを確認した後、`table1` と `table2` のデータをクエリするために [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md) を使用できます:

1. `table1` のクエリ:

```
   SELECT * FROM table1;
   +------+-------+-------+
   | id   | name  | score |
   +------+-------+-------+
   |    1 | Lily  |    21 |
   |    2 | Rose  |    22 |
   |    3 | Alice |    23 |
   |    4 | Julia |    24 |
   +------+-------+-------+
   4 rows in set (0.01 sec)
```

2. `table2` のクエリ:

```
   SELECT * FROM table2;
   +------+-------+-------+
   | id   | name  | score |
   +------+-------+-------+
   |    5 | Tony  |    25 |
   |    6 | Adam  |    26 |
   |    7 | Allen |    27 |
   |    8 | Jacky |    28 |
   +------+-------+-------+
   4 rows in set (0.01 sec)
```

## ロードジョブの表示

`information_schema` データベースの `loads` テーブルから、1つ以上のロードジョブの結果をクエリするために [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md) ステートメントを使用します。この機能はv3.1以降でサポートされています。

例1: `test_db` データベースで実行されたロードジョブの結果をクエリします。クエリステートメントでは、最大2つの結果を返すように指定し、返される結果は作成時刻 (`CREATE_TIME`) の降順でソートされるように指定します。

```
SELECT * FROM information_schema.loads
WHERE database_name = 'test_db'
ORDER BY create_time DESC
LIMIT 2\G
```

以下の結果が返されます:

```
*************************** 1. row ***************************
              JOB_ID: 20686
               LABEL: label_brokerload_unqualifiedtest_83
       DATABASE_NAME: test_db
               STATE: FINISHED
            PROGRESS: ETL:100%; LOAD:100%
                TYPE: BROKER
            PRIORITY: NORMAL
           SCAN_ROWS: 8
       FILTERED_ROWS: 0
     UNSELECTED_ROWS: 0
           SINK_ROWS: 8
            ETL_INFO:
           TASK_INFO: resource:N/A; timeout(s):14400; max_filter_ratio:1.0
         CREATE_TIME: 2023-08-02 15:25:22
      ETL_START_TIME: 2023-08-02 15:25:24
     ETL_FINISH_TIME: 2023-08-02 15:25:24
     LOAD_START_TIME: 2023-08-02 15:25:24
    LOAD_FINISH_TIME: 2023-08-02 15:25:27
         JOB_DETAILS: {"All backends":{"77fe760e-ec53-47f7-917d-be5528288c08":[10006],"0154f64e-e090-47b7-a4b2-92c2ece95f97":[10005]},"FileNumber":2,"FileSize":84,"InternalTableLoadBytes":252,"InternalTableLoadRows":8,"ScanBytes":84,"ScanRows":8,"TaskNumber":2,"Unfinished backends":{"77fe760e-ec53-47f7-917d-be5528288c08":[],"0154f64e-e090-47b7-a4b2-92c2ece95f97":[]}}
           ERROR_MSG: NULL
        TRACKING_URL: NULL
        TRACKING_SQL: NULL
REJECTED_RECORD_PATH: NULL
*************************** 2. row ***************************
              JOB_ID: 20624
               LABEL: label_brokerload_unqualifiedtest_82
       DATABASE_NAME: test_db
               STATE: FINISHED
            PROGRESS: ETL:100%; LOAD:100%
                TYPE: BROKER
            PRIORITY: NORMAL
           SCAN_ROWS: 12
       FILTERED_ROWS: 4
     UNSELECTED_ROWS: 0
           SINK_ROWS: 8
            ETL_INFO:
           TASK_INFO: resource:N/A; timeout(s):14400; max_filter_ratio:1.0
         CREATE_TIME: 2023-08-02 15:23:29
      ETL_START_TIME: 2023-08-02 15:23:34
     ETL_FINISH_TIME: 2023-08-02 15:23:34
     LOAD_START_TIME: 2023-08-02 15:23:34
    LOAD_FINISH_TIME: 2023-08-02 15:23:34
         JOB_DETAILS: {"All backends":{"78f78fc3-8509-451f-a0a2-c6b5db27dcb6":[10010],"a24aa357-f7de-4e49-9e09-e98463b5b53c":[10006]},"FileNumber":2,"FileSize":158,"InternalTableLoadBytes":333,"InternalTableLoadRows":8,"ScanBytes":158,"ScanRows":12,"TaskNumber":2,"Unfinished backends":{"78f78fc3-8509-451f-a0a2-c6b5db27dcb6":[],"a24aa357-f7de-4e49-9e09-e98463b5b53c":[]}}
           ERROR_MSG: NULL
        TRACKING_URL: http://172.26.195.69:8540/api/_load_error_log?file=error_log_78f78fc38509451f_a0a2c6b5db27dcb7
        TRACKING_SQL: select tracking_log from information_schema.load_tracking_logs where job_id=20624
REJECTED_RECORD_PATH: 172.26.95.92:/home/disk1/sr/be/storage/rejected_record/test_db/label_brokerload_unqualifiedtest_0728/6/404a20b1e4db4d27_8aa9af1e8d6d8bdc
```

例2: `test_db` データベースで実行されたロードジョブの結果 (ラベルが `label_brokerload_unqualifiedtest_82` のもの) をクエリします:

```
SELECT * FROM information_schema.loads
WHERE database_name = 'test_db' and label = 'label_brokerload_unqualifiedtest_82'\G
```

次の結果が返されます：

```
*************************** 1. row ***************************
              JOB_ID: 20624
               LABEL: label_brokerload_unqualifiedtest_82
       DATABASE_NAME: test_db
               STATE: FINISHED
            PROGRESS: ETL:100%; LOAD:100%
                TYPE: BROKER
            PRIORITY: NORMAL
           SCAN_ROWS: 12
       FILTERED_ROWS: 4
     UNSELECTED_ROWS: 0
           SINK_ROWS: 8
            ETL_INFO:
           TASK_INFO: resource:N/A; timeout(s):14400; max_filter_ratio:1.0
         CREATE_TIME: 2023-08-02 15:23:29
      ETL_START_TIME: 2023-08-02 15:23:34
     ETL_FINISH_TIME: 2023-08-02 15:23:34
     LOAD_START_TIME: 2023-08-02 15:23:34
    LOAD_FINISH_TIME: 2023-08-02 15:23:34
         JOB_DETAILS: {"All backends":{"78f78fc3-8509-451f-a0a2-c6b5db27dcb6":[10010],"a24aa357-f7de-4e49-9e09-e98463b5b53c":[10006]},"FileNumber":2,"FileSize":158,"InternalTableLoadBytes":333,"InternalTableLoadRows":8,"ScanBytes":158,"ScanRows":12,"TaskNumber":2,"Unfinished backends":{"78f78fc3-8509-451f-a0a2-c6b5db27dcb6":[],"a24aa357-f7de-4e49-9e09-e98463b5b53c":[]}}
           ERROR_MSG: NULL
        TRACKING_URL: http://172.26.195.69:8540/api/_load_error_log?file=error_log_78f78fc38509451f_a0a2c6b5db27dcb7
        TRACKING_SQL: select tracking_log from information_schema.load_tracking_logs where job_id=20624
REJECTED_RECORD_PATH: 172.26.95.92:/home/disk1/sr/be/storage/rejected_record/test_db/label_brokerload_unqualifiedtest_0728/6/404a20b1e4db4d27_8aa9af1e8d6d8bdc
```

返される結果のフィールドに関する情報については、[Information Schema > loads](../administration/information_schema.md#loads)を参照してください。

## ロードジョブのキャンセル

ロードジョブが**CANCELLED**または**FINISHED**のステージにない場合、[CANCEL LOAD](../sql-reference/sql-statements/data-manipulation/CANCEL%20LOAD.md)ステートメントを使用してジョブをキャンセルできます。

たとえば、データベース`test_db`内のラベルが`label1`であるロードジョブをキャンセルするには、次のステートメントを実行できます：

```
CANCEL LOAD
FROM test_db
WHERE LABEL = "label1";
```

## ジョブの分割と並行実行

ブローカーロードジョブは、複数のタスクに分割されて並行して実行されることがあります。ロードジョブ内のタスクは、単一のトランザクション内で実行されます。すべてのタスクは成功するか失敗するかのいずれかです。StarRocksは、`LOAD`ステートメントで`data_desc`を宣言する方法に基づいて、各ロードジョブを分割します：

- 異なるテーブルを指定する各`data_desc`パラメータを宣言する場合、各テーブルのデータをロードするためのタスクが生成されます。

- 同じテーブルの異なるパーティションを指定する各`data_desc`パラメータを宣言する場合、各パーティションのデータをロードするためのタスクが生成されます。

さらに、各タスクは1つ以上のインスタンスにさらに分割され、これらはStarRocksクラスタのBEに均等に分散されて並行して実行されます。StarRocksは、次の[FEの設定](../administration/Configuration.md#fe-configuration-items)に基づいて各タスクを分割します：

- `min_bytes_per_broker_scanner`：各インスタンスが処理する最小データ量。デフォルトは64 MBです。

- `load_parallel_instance_num`：個々のBE上の各ロードジョブで許可される並行インスタンスの数。デフォルトは1です。

  個々のタスク内のインスタンス数を計算するために、次の式を使用できます：

  **個々のタスク内のインスタンス数 = 個々のタスクがロードするデータ量/`min_bytes_per_broker_scanner`、`load_parallel_instance_num` x BEの数**

ほとんどの場合、各ロードジョブには1つの`data_desc`が宣言され、各ロードジョブは1つのタスクにのみ分割され、タスクはBEの数と同じ数のインスタンスに分割されます。

## 関連する設定項目

[FEの設定項目](../administration/Configuration.md#fe-configuration-items)である `max_broker_load_job_concurrency` は、StarRocksクラスタ内で同時に実行できるブローカーロードジョブの最大数を指定します。

StarRocks v2.4以前では、特定の時間内に提出されたブローカーロードジョブの総数が最大数を超える場合、余剰のジョブはキューに入れられ、提出時刻に基づいてスケジュールされます。

StarRocks v2.5以降では、特定の時間内に提出されたブローカーロードジョブの総数が最大数を超える場合、余剰のジョブは優先度に基づいてキューに入れられ、スケジュールされます。ジョブの作成時に `priority` パラメータを使用してジョブの優先度を指定することができます。詳細は [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md#opt_properties) を参照してください。また、**QUEUEING** または **LOADING** 状態にある既存のジョブの優先度を変更するために [ALTER LOAD](../sql-reference/sql-statements/data-manipulation/ALTER%20LOAD.md) を使用することもできます。
