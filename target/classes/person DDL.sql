
-- replace "poc" with the dataset name

CREATE TABLE poc.person
(
  id   INT64,
  name STRING,
  addresses Array<STRUCT<type STRING, address STRING>>
)