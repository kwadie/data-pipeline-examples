/*
 *
 *  * Copyright (C) 2020 Google Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  * use this file except in compliance with the License. You may obtain a copy of
 *  * the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  * License for the specific language governing permissions and limitations under
 *  * the License.
 *
 */

package com.google.cloud.pso.pipeline.examples;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.pso.pipeline.model.Person;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The batch pipeline reads a nested Xml Schema, converts to BigQuery schema it and load it.
 * <p>
 * usage:
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.pso.pipeline.examples.XmlPipeline \
 * -Dexec.args=" \
 * --sourcePath=$xmlPath \
 * --outputTableSpec=$project:dataset.table \
 * --project=$projectToRunDataflow \
 * --tempLocation=$gs://bucket \
 * --runner=$DirectRunnerOrDataflowRunner"
 */


public class XmlPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(XmlPipeline.class);

    public static TableSchema getTableSchema() {
        return destinationSchema;
    }

    public interface Options extends DataflowPipelineOptions {
        @Description("source data path")
        @Validation.Required
        String getSourcePath();

        void setSourcePath(String sourcePath);

        @Description("Table spec to write the output to in the format project:dataset.table ")
        String getOutputTableSpec();

        void setOutputTableSpec(String value);
    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(Options.class);

        Options options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }


    // encapsulate the pipeline logic here for easier unit testing
    public static PipelineResult run(Options options) {

        Pipeline p = Pipeline.create(options);

        p
                .apply("Extract from XML",
                        XmlIO.<Person>read()
                                .from(options.getSourcePath())
                                .withRootElement("people")
                                .withRecordElement("person")
                                .withRecordClass(Person.class))
                .apply("Load to BQ",
                        BigQueryIO.<Person>write()
                                .to(options.getOutputTableSpec())
                                // only a SimpleFunction is needed for formatting
                                // DoFn with ParDo could be used in a previous step for complex processing
                                .withFormatFunction(new FormatPersonAsTableRow())
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                // when no schema is provided the CreateDisposition must be CREATE_NEVER
                                //.withSchema(getTableSchema())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withExtendedErrorInfo()
                );

        return p.run();
    }

    public static class FormatPersonAsTableRow extends SimpleFunction<Person, TableRow> {
        @Override
        public TableRow apply(Person p) {
            return new TableRow()
                    .set("name", p.name)
                    .set("id", p.id)
                    .set("addresses", p.addressList);
        }
    }

    // destination BigQuery table schema
    private static final TableSchema destinationSchema = new TableSchema().setFields(
            ImmutableList.of(
                    new TableFieldSchema().setName("id").setType("INT64"),
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("addresses")
                            .setType("RECORD")
                            .setFields(ImmutableList.of(
                                    new TableFieldSchema().setName("type").setType("STRING"),
                                    new TableFieldSchema().setName("address").setType("STRING")
                            ))
            )
    );
}
