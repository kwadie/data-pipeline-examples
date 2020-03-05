
# Data pipeline examples
Sample code and blueprints for Apache Beam / Cloud Dataflow

## Copyright
Copyright (C) 2020 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this code except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

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