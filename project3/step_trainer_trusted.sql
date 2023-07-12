CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `sensorReadingTime` bigint COMMENT 'from deserializer',
  `serialNumber` string COMMENT 'from deserializer',
  `distanceFromObject` int COMMENT 'from deserializer'
  )
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'dots.in.keys'='FALSE', 
  'ignore.malformed.json'='FALSE', 
  'mapping'='TRUE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://udacity-labs-stedi-trevor/step_trainer_trusted'
TBLPROPERTIES (
  'classification'='json', 
  'transient_lastDdlTime'='1689165531')
