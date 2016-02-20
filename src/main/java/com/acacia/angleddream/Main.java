/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.acacia.angleddream;

import com.acacia.angleddream.common.*;

import com.acacia.sdk.AbstractTransformComposer;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipeline;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.gson.Gson;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class Main {


    /**
     * Sets up and starts streaming pipeline.
     * <*
     *
     * @throws IOException if there is a problem setting up resources
     */
    public static void main(String[] args) throws IOException {


        ComposerManagerOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(ComposerManagerOptions.class);
        options.setStreaming(true);
        options.setRunner(DataflowPipelineRunner.class);

        List<String> outputTopics = new ArrayList<>();


        if (options.getOutputTopics() != null) {
            outputTopics = Arrays.asList(options.getOutputTopics().split(","));
        }

        DataflowUtils dataflowUtils = new DataflowUtils(options);
        dataflowUtils.setup();

        Pipeline pipeline = DataflowPipeline.create(options);

        Map<String, String> containerIPs = new HashMap<>();

        if (options.getContainerDeps() != null) {
            String[] deps = options.getContainerDeps().split(",");
            for (String dep : deps) {
                String[] item = dep.split("\\|");
                containerIPs.put(item[0], item[1]);
            }

        }


        Map<String, String> mapargs = new HashMap<>();
        mapargs.putAll(containerIPs);

        MultiTransform mt = new MultiTransform(mapargs);
        //List<PCollectionTuple> tuples = new ArrayList<>();
        PCollectionTuple t = PCollectionTuple.empty(pipeline);


        if (options.getPubsubTopic() != null) {
            List<String> inputs = Arrays.asList();


               t = pipeline.apply(PubsubIO.Read.idLabel(UUID.randomUUID().toString()).named("read-" + options.getPubsubTopic()).topic(options.getPubsubTopic())).apply(mt);


        } else {
            //do batch stuff delete this
               t = pipeline.apply(PubsubIO.Read.idLabel(UUID.randomUUID().toString()).named("read-" + options.getPubsubTopic()).topic(options.getPubsubTopic())).apply(mt);
        }


        if (!outputTopics.isEmpty()) {



                try {

                    if (t.get(Tags.mainOutput) != null) {

                        for (String topic : outputTopics) {
                            t.get(Tags.mainOutput).apply(PubsubIO.Write.topic(topic));
                        }
                    }
                    if (t.get(Tags.errorOutput) != null) {
                        t.get(Tags.errorOutput).apply(PubsubIO.Write.topic(options.getErrorPipelineName()));
                    }

                } catch (NullPointerException e) {
                    System.out.println("Exception: make sure PubsubTopic is not empty, and pipeline JAR file is on classpath, correctly named, correctly built, and in the correct bucket");
                }

        }


        if (options.getBigQueryTable() != null) {

            //"BigQuery table to write to, specified as
            // "<project_id>:<dataset_id>.<table_id>. The dataset must already exist."

            //TODO: error handling on bigquery writes?


            String bqRef = options.getProject() + ":" + options.getBigQueryDataset() + "." + options.getBigQueryTable();


            FileReader schemaSource = new FileReader(options.getBigQuerySchema());

            List<TableFieldSchema> fields = (new Gson())
                    .<List<TableFieldSchema>>fromJson(schemaSource,
                            (new ArrayList<TableFieldSchema>()).getClass());

            TableSchema schema = new TableSchema().setFields(fields);


            pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic()))
                    .apply(ParDo.of(new BigQueryProcessor()))
                    .apply(BigQueryIO.Write
                            .to(bqRef)
                            .withSchema(schema)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


        }
        PipelineResult result = pipeline.run();

    }


}