
# Data pipeline examples
Sample code and blueprints for Apache Beam / Cloud Dataflow

## Copyright
Copyright 2020 Google LLC. This software is provided as-is, without warranty or representation for any use or purpose. Your use of it is subject to your agreement with Google.

## XmlPipeline
Reads a nested XML file ([sample](src/test/resources/people.xml)) and load it to BigQuery
* Create the destination table on BigQuery via this [DDL](src/main/resources/person DDL.sql)
* Run the job via DirectRunner or DataflowRunner
```
  mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.pso.pipeline.examples.XmlPipeline \
  -Dexec.args=" \
  --sourcePath=$xmlPath \
  --outputTableSpec=$gcpProject:dataset.table \
  --project=$gcpProjectIDToRunDataflow \
  --tempLocation=$gs://bucket \
  --runner=$DirectRunnerOrDataflowRunner"
```
* PS: The truncate the destination table at each run