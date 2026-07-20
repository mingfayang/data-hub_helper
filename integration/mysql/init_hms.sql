DROP DATABASE IF EXISTS hive_metastore_test;
CREATE DATABASE hive_metastore_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
DROP USER IF EXISTS 'hive_test'@'localhost';
CREATE USER 'hive_test'@'localhost' IDENTIFIED BY 'hive_test_password';
GRANT SELECT ON hive_metastore_test.* TO 'hive_test'@'localhost';
USE hive_metastore_test;

CREATE TABLE DBS (
  DB_ID BIGINT PRIMARY KEY,
  `DESC` VARCHAR(4000),
  DB_LOCATION_URI VARCHAR(4000),
  NAME VARCHAR(256) NOT NULL,
  OWNER_NAME VARCHAR(128),
  OWNER_TYPE VARCHAR(10),
  CTLG_NAME VARCHAR(256)
);

CREATE TABLE TBLS (
  TBL_ID BIGINT PRIMARY KEY,
  CREATE_TIME INT,
  DB_ID BIGINT NOT NULL,
  LAST_ACCESS_TIME INT,
  OWNER VARCHAR(767),
  RETENTION INT,
  SD_ID BIGINT,
  TBL_NAME VARCHAR(256) NOT NULL,
  TBL_TYPE VARCHAR(128),
  VIEW_EXPANDED_TEXT MEDIUMTEXT,
  VIEW_ORIGINAL_TEXT MEDIUMTEXT,
  IS_REWRITE_ENABLED BIT,
  INDEX IDX_TBLS_DB_ID (DB_ID),
  INDEX IDX_TBLS_SD_ID (SD_ID)
);

CREATE TABLE SDS (
  SD_ID BIGINT PRIMARY KEY,
  CD_ID BIGINT,
  INPUT_FORMAT VARCHAR(4000),
  IS_COMPRESSED BIT,
  LOCATION VARCHAR(4000),
  NUM_BUCKETS INT,
  OUTPUT_FORMAT VARCHAR(4000),
  SERDE_ID BIGINT,
  STORED_AS_SUB_DIRECTORIES BIT,
  INDEX IDX_SDS_CD_ID (CD_ID)
);

CREATE TABLE COLUMNS_V2 (
  CD_ID BIGINT NOT NULL,
  COMMENT VARCHAR(256),
  COLUMN_NAME VARCHAR(512) NOT NULL,
  TYPE_NAME MEDIUMTEXT NOT NULL,
  INTEGER_IDX INT NOT NULL,
  PRIMARY KEY (CD_ID, COLUMN_NAME)
);

CREATE TABLE PARTITION_KEYS (
  TBL_ID BIGINT NOT NULL,
  PKEY_COMMENT VARCHAR(4000),
  PKEY_NAME VARCHAR(128) NOT NULL,
  PKEY_TYPE VARCHAR(767) NOT NULL,
  INTEGER_IDX INT NOT NULL,
  PRIMARY KEY (TBL_ID, PKEY_NAME)
);

CREATE TABLE TABLE_PARAMS (
  TBL_ID BIGINT NOT NULL,
  PARAM_KEY VARCHAR(256) NOT NULL,
  PARAM_VALUE MEDIUMTEXT,
  PRIMARY KEY (TBL_ID, PARAM_KEY)
);

CREATE TABLE DATABASE_PARAMS (
  DB_ID BIGINT NOT NULL,
  PARAM_KEY VARCHAR(180) NOT NULL,
  PARAM_VALUE VARCHAR(4000),
  PRIMARY KEY (DB_ID, PARAM_KEY)
);

CREATE TABLE SERDES (
  SERDE_ID BIGINT PRIMARY KEY,
  NAME VARCHAR(128),
  SLIB VARCHAR(4000),
  DESCRIPTION VARCHAR(4000),
  SERIALIZER_CLASS VARCHAR(4000),
  DESERIALIZER_CLASS VARCHAR(4000),
  SERDE_TYPE INT
);

CREATE TABLE SERDE_PARAMS (
  SERDE_ID BIGINT NOT NULL,
  PARAM_KEY VARCHAR(256) NOT NULL,
  PARAM_VALUE MEDIUMTEXT,
  PRIMARY KEY (SERDE_ID, PARAM_KEY)
);

INSERT INTO DBS VALUES
  (1, 'Local Hive integration database', 'file:///tmp/hive/warehouse', 'hive', 'hive_admin', 'USER', 'hive'),
  (2, 'Must be filtered out', 'file:///tmp/ignored', 'ignored', 'nobody', 'USER', 'hive');

INSERT INTO SDS VALUES
  (20, 30, 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat', 0, 'file:///tmp/hive/warehouse/orders', 0, 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat', 50, 0),
  (21, 31, 'org.apache.hadoop.mapred.TextInputFormat', 0, 'file:///tmp/hive/warehouse/customers', 0, 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 51, 0),
  (22, 32, 'org.apache.hadoop.mapred.TextInputFormat', 0, 'file:///tmp/hive/warehouse/order_view', 0, 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 52, 0),
  (23, 33, 'org.apache.hadoop.mapred.TextInputFormat', 0, 'file:///tmp/ignored/not_exported', 0, 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 53, 0);

INSERT INTO TBLS VALUES
  (10, 1710000000, 1, 0, 'alice', 0, 20, 'orders', 'EXTERNAL_TABLE', NULL, NULL, 0),
  (11, 1710000100, 1, 0, 'bob', 0, 21, 'customers', 'MANAGED_TABLE', NULL, NULL, 0),
  (12, 1710000200, 1, 0, 'carol', 0, 22, 'order_view', 'VIRTUAL_VIEW', 'SELECT id, amount FROM hive.orders', 'SELECT id, amount FROM orders', 0),
  (13, 1710000300, 2, 0, 'nobody', 0, 23, 'not_exported', 'MANAGED_TABLE', NULL, NULL, 0);

INSERT INTO COLUMNS_V2 VALUES
  (30, 'Order identifier', 'id', 'bigint', 0),
  (30, 'Order amount', 'amount', 'decimal(10,2)', 1),
  (31, 'Customer identifier', 'customer_id', 'bigint', 0),
  (31, 'Customer name', 'name', 'string', 1),
  (32, 'Order identifier', 'id', 'bigint', 0),
  (32, 'Order amount', 'amount', 'decimal(10,2)', 1),
  (33, 'Ignored field', 'x', 'string', 0);

INSERT INTO PARTITION_KEYS VALUES
  (10, 'Partition date', 'dt', 'date', 10);

INSERT INTO TABLE_PARAMS VALUES
  (10, 'comment', 'Orders from the integration test'),
  (10, 'classification', 'gold'),
  (11, 'comment', 'Customer master data'),
  (12, 'comment', 'Order projection view');

INSERT INTO SERDES VALUES
  (50, 'orders_serde', 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe', NULL, NULL, NULL, 0),
  (51, 'customers_serde', 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', NULL, NULL, NULL, 0),
  (52, 'view_serde', 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', NULL, NULL, NULL, 0),
  (53, 'ignored_serde', 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', NULL, NULL, NULL, 0);

INSERT INTO SERDE_PARAMS VALUES
  (50, 'serialization.format', '1'),
  (51, 'field.delim', ','),
  (52, 'serialization.format', '1'),
  (53, 'serialization.format', '1');
