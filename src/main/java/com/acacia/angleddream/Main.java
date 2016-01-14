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

import com.acacia.sdk.AbstractTransform;
import com.acacia.sdk.AbstractTransformComposer;
import com.acacia.sdk.TempOrionTableSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipeline;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;

import java.io.IOException;
import java.net.URLClassLoader;
import java.util.*;

/**
 * <p>This pipeline example reads lines of text from a PubSub topic, splits each line
 * into individual words, capitalizes those words, and writes the output to
 * a BigQuery table.
 * <p>
 * <p>By default, the example will inject the data from the the Pub/Sub {@literal --pubsubTopic}.
 * It will make it available for the streaming pipeline to process.
 */
public class Main {


    /**
     * Sets up and starts streaming pipeline.
     * <p>
     * NOTE: needs to take some kind of exectution graph as an argument? paths to oooo
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

        List<String> executionPipelineClasses = new ArrayList<>();
        if (options.getExecutionPipelineClasses() != null) {
            executionPipelineClasses = Arrays.asList(options.getExecutionPipelineClasses().split(","));
        }


        List<Class<?>> transforms = new ArrayList<>();

        //do this temporarily until
        DataflowUtils dataflowUtils = new DataflowUtils(options);
        dataflowUtils.setup();

        //NOTE -- ALWAYS BUNDLE DEPENDENCIES IN CLASS JARS?


        ServiceLoader<AbstractTransformComposer> loader = null;
        loader = ServiceLoader.load(AbstractTransformComposer.class, ClassLoader.getSystemClassLoader());

        List<AbstractTransformComposer> transformComposers = new ArrayList<>();

        Iterator<AbstractTransformComposer> transformsf = loader.iterator();

        while (transformsf.hasNext()) {

            AbstractTransformComposer c = transformsf.next();

            transformComposers.add(c);

            for (AbstractTransform t : c.getOrderedTransforms()) {

                System.out.println("Examining: " + t.getClass().getCanonicalName());
                if (executionPipelineClasses.contains(t.getClass().getCanonicalName())) {
                    System.out.println("Loading: " + t.getClass().getCanonicalName());
                }

            }

        }

        //TODO: change multiwrite to side outputs?

        Pipeline pipeline = DataflowPipeline.create(options);

        //need to support more than ParDo.of in scaffolding

        //need to check for proper things in classpath etc so people don't try to run w/ 0 pipelines

        if (!outputTopics.isEmpty()) {

            PCollectionTuple t = pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic())).apply(new MultiTransform());

            //how to abstract out -- make sure everything just returns a PCollection or PCollectionTuple?


            if (t.get(Tags.mainOutput) != null) {

                for(String topic : outputTopics){
                    t.get(Tags.mainOutput).apply(PubsubIO.Write.topic(topic));

                }

            }

            if (t.get(Tags.errorOutput) != null) {
                t.get(Tags.errorOutput).apply(PubsubIO.Write.topic(options.getErrorPipelineName()));
            }
        }

        if (options.getBigQueryTable() != null) {

            //"BigQuery table to write to, specified as
            // "<project_id>:<dataset_id>.<table_id>. The dataset must already exist."


            String bqRef = options.getProject() + ":" + options.getBigQueryDataset() + "." + options.getBigQueryTable();

            TempOrionTableSchema ts = new TempOrionTableSchema();


            PCollectionTuple t = pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic()))
                    .apply(new MultiTransform());

            if (t.get(Tags.mainOutput) != null) {
                t.get(Tags.mainOutput).apply(ParDo.of(new BigQueryProcessor()))
                        .apply(BigQueryIO.Write
                                .to(bqRef)
                                .withSchema(ts.getOrionTS())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

            }

            if (t.get(Tags.errorOutput) != null) {
                t.get(Tags.errorOutput).apply(PubsubIO.Write.topic(options.getErrorPipelineName()));
            }


        }
        PipelineResult result = pipeline.run();
    }


}