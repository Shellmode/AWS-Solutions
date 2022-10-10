CREATE EXTERNAL TABLE IF NOT EXISTS `vpcflowlogdb`.`vpcflowlogtable` (
  `interface-id` string,
  `srcaddr` string,
  `srcport` string,
  `pkt-src-aws-service` string,
  `dstaddr` string,
  `dstport` string,
  `pkt-dst-aws-service` string,
  `bytes` bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ' ',
  'field.delim' = ' ',
  'collection.delim' = '\u0002',
  'mapkey.delim' = '\u0003'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://vpcflowlog-nat/';
