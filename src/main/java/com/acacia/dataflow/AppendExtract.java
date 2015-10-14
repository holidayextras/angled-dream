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

package com.acacia.dataflow;

import com.acacia.dataflow.common.DataflowUtils;
import com.acacia.dataflow.common.MultiWrite;
import com.acacia.dataflow.common.PipelineComposerOptions;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.acacia.dataflow.common.PubsubTopicOptions;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 *
 * <p>This pipeline example reads lines of text from a PubSub topic, splits each line
 * into individual words, capitalizes those words, and writes the output to
 * a BigQuery table.
 *
 * <p>By default, the example will inject the data from the the Pub/Sub {@literal --pubsubTopic}.
 * It will make it available for the streaming pipeline to process.
 *
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

  }

  /**
   * Options supported by {@link AppendExtract}.
   *
   * <p>Inherits standard configuration options.
   */
  private interface ComposerManagerOptions
      extends PubsubTopicOptions, PipelineComposerOptions {
  }

  /**
   * Sets up and starts streaming pipeline.
   *
   * @throws IOException if there is a problem setting up resources
   */
  public static void main(String[] args) throws IOException {
    ComposerManagerOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(ComposerManagerOptions.class);
      options.setStreaming(true);
      options.setPubsubTopic("projects/hx-test/topics/data-topic");
    // In order to cancel the pipelines automatically,
    // {@literal DataflowPipelineRunner} is forced to be used.
      options.setRunner(DataflowPipelineRunner.class);
      options.setMaxNumWorkers(1);
      options.setNumWorkers(1);
      options.setWorkerMachineType("n1-standard-1");

      String[] outputTopics = options.getOutputTopics().split(",");
      String[] executionPipelineClasses = options.getExecutionPipelineClasses().split(",");

      DataflowUtils dataflowUtils = new DataflowUtils(options);
      dataflowUtils.setup();




      //need to setup error topic too
      //derived from unique name?

      Pipeline pipeline = Pipeline.create(options);
      pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic()))
              .apply(ParDo.of(new Append()))
      .apply(MultiWrite.topics(Arrays.asList(outputTopics)));

       PipelineResult result = pipeline.run();

  }

}
