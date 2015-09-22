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

package com.acacia.dataflowtest;

import com.acacia.dataflowtest.common.DataflowUtils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.acacia.dataflowtest.common.ExampleBigQueryTableOptions;
import com.acacia.dataflowtest.common.ExamplePubsubTopicOptions;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A streaming Dataflow Example using BigQuery output.
 *
 * <p>This pipeline example reads lines of text from a PubSub topic, splits each line
 * into individual words, capitalizes those words, and writes the output to
 * a BigQuery table.
 *
 * <p>By default, the example will run a separate pipeline to inject the data from the default
 * {@literal --inputFile} to the Pub/Sub {@literal --pubsubTopic}. It will make it available for
 * the streaming pipeline to process. You may override the default {@literal --inputFile} with the
 * file of your choosing. You may also set {@literal --inputFile} to an empty string, which will
 * disable the automatic Pub/Sub injection, and allow you to use separate tool to control the input
 * to this example.
 *
 * <p>The example is configured to use the default Pub/Sub topic and the default BigQuery table
 * from the example common package (there are no defaults for a general Dataflow pipeline).
 * You can override them by using the {@literal --pubsubTopic}, {@literal --bigQueryDataset}, and
 * {@literal --bigQueryTable} options. If the Pub/Sub topic or the BigQuery table do not exist,
 * the example will try to create them.
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class AppendExtract {


  /** A DoFn that uppercases a word. */
  static class Append extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element() + "w00t");
    }
  }


    /**
     * Converts strings into BigQuery rows.
     */
    static class StringToRowConverter extends DoFn<String, TableRow> {
        /**
         * In this example, put the whole string into single BigQuery field.
         */
        @Override
        public void processElement(ProcessContext c) {
            c.output(new TableRow().set("string_field", c.element()));
        }

        static TableSchema getSchema() {
            return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
                // Compose the list of TableFieldSchema from tableSchema.
                {
                    add(new TableFieldSchema().setName("string_field").setType("STRING"));
                }
            });
        }
    }


    /**

  }

  /**
   * Options supported by {@link AppendExtract}.
   *
   * <p>Inherits standard configuration options.
   */
  private interface StreamingWordExtractOptions
      extends ExamplePubsubTopicOptions, ExampleBigQueryTableOptions {
  }

  /**
   * Sets up and starts streaming pipeline.
   *
   * @throws IOException if there is a problem setting up resources
   */
  public static void main(String[] args) throws IOException {
    StreamingWordExtractOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(StreamingWordExtractOptions.class);
      options.setStreaming(true);
      options.setPubsubTopic("projects/hx-test/topics/data-topic");
    // In order to cancel the pipelines automatically,
    // {@literal DataflowPipelineRunner} is forced to be used.
      options.setRunner(DataflowPipelineRunner.class);
      options.setBigQuerySchema(StringToRowConverter.getSchema());
      options.setMaxNumWorkers(1);
      options.setNumWorkers(1);
      options.setWorkerMachineType("n1-standard-1");

      DataflowUtils dataflowUtils = new DataflowUtils(options);
      dataflowUtils.setup();

      String tableSpec = new StringBuilder()
              .append(options.getProject()).append(":")
              .append(options.getBigQueryDataset()).append(".")
              .append(options.getBigQueryTable())
              .toString();

      Pipeline pipeline = Pipeline.create(options);
      pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic()))
              .apply(ParDo.of(new Append()))
              .apply(ParDo.of(new StringToRowConverter()))
              .apply(BigQueryIO.Write.to(tableSpec)
                      .withSchema(StringToRowConverter.getSchema()));


    PipelineResult result = pipeline.run();


  }
}
