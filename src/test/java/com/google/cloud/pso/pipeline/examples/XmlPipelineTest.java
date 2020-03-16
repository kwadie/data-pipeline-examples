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
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.pso.pipeline.model.Address;
import com.google.cloud.pso.pipeline.model.Person;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class XmlPipelineTest {

    @org.junit.Test
    public void testFormatPersonAsTableRow() {

        List<Address> lukeAddresses = new ArrayList<>();
        lukeAddresses.add(new Address("Tatooine", "home"));
        lukeAddresses.add(new Address("Lars Farm", "work"));

        Person luke = new Person("Luke Skywalker",1, lukeAddresses);

        TableRow expctedOutput = new TableRow()
                .set("name", "Luke Skywalker")
                .set("id", 1)
                .set("addresses", lukeAddresses);

        XmlPipeline.FormatPersonAsTableRow formatter = new XmlPipeline.FormatPersonAsTableRow();
        TableRow output = formatter.apply(luke);

        Assert.assertEquals(output, expctedOutput);
    }

}
