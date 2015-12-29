/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.acacia.angleddream.common;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.util.Lists;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.Topic;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.util.Transport;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

/**
 * The utility class that sets up and tears down external resources, starts the Google Cloud Pub/Sub
 * injector, and cancels the streaming and the injector pipelines once the program terminates.
 *
 * <p>It is used to run Dataflow examples, such as TrafficMaxLaneFlow and TrafficRoutes.
 */
public class DataflowUtils {

  private final DataflowPipelineOptions options;

  private Pubsub pubsubClient = null;
  private Dataflow dataflowClient = null;

  private List<String> pendingMessages = Lists.newArrayList();

  /**
   * Define an interface that supports the PubSub and BigQuery example options.
   */
  public static interface DataflowUtilsOptions
        extends PubsubTopicOptions {
  }

  public DataflowUtils(DataflowPipelineOptions options) {
    this.options = options;
  }

  /**
   * Do resources and runner options setup.
   */
  public DataflowUtils(DataflowPipelineOptions options, boolean isUnbounded)
      throws IOException {
    this.options = options;
    setupResourcesAndRunner(isUnbounded);
  }

  /**
   * Sets up external resources that are required by the example,
   * such as Pub/Sub topics and BigQuery tables.
   *
   * @throws IOException if there is a problem setting up the resources
   */
  public void setup() throws IOException {
    setupPubsubTopics();

  }

  /**
   * Set up external resources, and configure the runner appropriately.
   */
  public void setupResourcesAndRunner(boolean isUnbounded) throws IOException {
    if (isUnbounded) {
      options.setStreaming(true);
    }
    setup();
    setupRunner();
  }

  /**
   * Sets up the Google Cloud Pub/Sub topic.
   *
   * <p>If the topic doesn't exist, a new topic with the given name will be created.
   *
   * @throws IOException if there is a problem setting up the Pub/Sub topic
   */
  public void setupPubsubTopics() throws IOException {
    PubsubTopicOptions pubsubTopicOptions = options.as(PubsubTopicOptions.class);
    if (!pubsubTopicOptions.getPubsubTopic().isEmpty()) {
      pendingMessages.add("*******************Set Up Input Pubsub Topic*********************");
      setupPubsubTopics(pubsubTopicOptions.getPubsubTopic());


      pendingMessages.add("The Pub/Sub topic has been set up for this example: "
          + pubsubTopicOptions.getPubsubTopic());
    }
    PipelineComposerOptions pipelineComposerOptions = options.as(PipelineComposerOptions.class);
    if ((pipelineComposerOptions.getOutputTopics() != null) && !pipelineComposerOptions.getOutputTopics().isEmpty()) {

      for(String t : pipelineComposerOptions.getOutputTopics().split(",")) {

        pendingMessages.add("*******************Set Up Output Pubsub Topics*********************");
        setupPubsubTopics(t);
        pendingMessages.add("The Pub/Sub topic has been set up for this example: "
                + pubsubTopicOptions.getPubsubTopic());
      }
    }
    else{
      pendingMessages.add("Weird: no output topics");
      System.out.println("weird: no output topics?");
    }



  }


  /**
   * Tears down external resources that can be deleted upon the example's completion.
   */
  private void tearDown() {
    pendingMessages.add("*************************Tear Down*************************");
  }



  private void setupPubsubTopics(String topic) throws IOException {
    if (pubsubClient == null) {
      pubsubClient = Transport.newPubsubClient(options).build();
    }
    if (executeNullIfNotFound(pubsubClient.projects().topics().get(topic)) == null) {
      pubsubClient.projects().topics().create(topic, new Topic().setName(topic)).execute();
    }
  }


  /**
   * Do some runner setup: check that the DirectPipelineRunner is not used in conjunction with
   * streaming, and if streaming is specified, use the DataflowPipelineRunner. Return the streaming
   * flag value.
   */
  public void setupRunner() {
    if (options.isStreaming()) {
      if (options.getRunner() == DirectPipelineRunner.class) {
        throw new IllegalArgumentException(
          "Processing of unbounded input sources is not supported with the DirectPipelineRunner.");
      }
      // In order to cancel the pipelines automatically,
      // {@literal DataflowPipelineRunner} is forced to be used.
      options.setRunner(DataflowPipelineRunner.class);
    }
  }



  private static <T> T executeNullIfNotFound(
      AbstractGoogleClientRequest<T> request) throws IOException {
    try {
      return request.execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == HttpServletResponse.SC_NOT_FOUND) {
        return null;
      } else {
        throw e;
      }
    }
  }
}
