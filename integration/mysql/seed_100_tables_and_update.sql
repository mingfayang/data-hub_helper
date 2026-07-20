USE hive_metastore_test;

DELIMITER //
CREATE PROCEDURE seed_100_datahub_tables()
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= 100 DO
    INSERT INTO SERDES (
      SERDE_ID, NAME, SLIB, DESCRIPTION, SERIALIZER_CLASS, DESERIALIZER_CLASS, SERDE_TYPE
    ) VALUES (
      1000 + i,
      CONCAT('test_table_', LPAD(i, 3, '0'), '_serde'),
      'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
      NULL, NULL, NULL, 0
    );

    INSERT INTO SERDE_PARAMS (SERDE_ID, PARAM_KEY, PARAM_VALUE) VALUES
      (1000 + i, 'serialization.format', '1');

    INSERT INTO SDS (
      SD_ID, CD_ID, INPUT_FORMAT, IS_COMPRESSED, LOCATION, NUM_BUCKETS,
      OUTPUT_FORMAT, SERDE_ID, STORED_AS_SUB_DIRECTORIES
    ) VALUES (
      1000 + i,
      2000 + i,
      'org.apache.hadoop.mapred.TextInputFormat',
      0,
      CONCAT('file:///tmp/hive/warehouse/test_table_', LPAD(i, 3, '0')),
      0,
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
      1000 + i,
      0
    );

    INSERT INTO TBLS (
      TBL_ID, CREATE_TIME, DB_ID, LAST_ACCESS_TIME, OWNER, RETENTION, SD_ID,
      TBL_NAME, TBL_TYPE, VIEW_EXPANDED_TEXT, VIEW_ORIGINAL_TEXT, IS_REWRITE_ENABLED
    ) VALUES (
      1000 + i,
      1710100000 + i,
      1,
      0,
      'integration_owner',
      0,
      1000 + i,
      CONCAT('test_table_', LPAD(i, 3, '0')),
      'MANAGED_TABLE',
      NULL,
      NULL,
      0
    );

    INSERT INTO COLUMNS_V2 (CD_ID, COMMENT, COLUMN_NAME, TYPE_NAME, INTEGER_IDX) VALUES
      (2000 + i, CONCAT('Initial id column ', LPAD(i, 3, '0')), 'id', 'bigint', 0),
      (2000 + i, CONCAT('Initial value column ', LPAD(i, 3, '0')), 'value', 'string', 1);

    INSERT INTO TABLE_PARAMS (TBL_ID, PARAM_KEY, PARAM_VALUE) VALUES
      (1000 + i, 'comment', CONCAT('Initial integration table ', LPAD(i, 3, '0'))),
      (1000 + i, 'classification', 'integration_bulk');

    SET i = i + 1;
  END WHILE;
END //
DELIMITER ;

CALL seed_100_datahub_tables();
DROP PROCEDURE seed_100_datahub_tables;

UPDATE TABLE_PARAMS
SET PARAM_VALUE = 'Updated integration table 042 comment'
WHERE TBL_ID = 1042 AND PARAM_KEY = 'comment';

UPDATE SDS
SET LOCATION = 'file:///tmp/hive/warehouse/test_table_042_updated'
WHERE SD_ID = 1042;

UPDATE COLUMNS_V2
SET COMMENT = 'Updated value column 042'
WHERE CD_ID = 2042 AND COLUMN_NAME = 'value';
