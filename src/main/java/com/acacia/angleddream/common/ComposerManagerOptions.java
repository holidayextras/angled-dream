package com.acacia.angleddream.common;

import com.google.cloud.dataflow.examples.common.ExampleBigQueryTableOptions;


/**
 * Options supported by pipelines.
 * <p>
 * <p>Inherits standard configuration options.
 */
public interface ComposerManagerOptions
        extends PubsubTopicOptions, PipelineComposerOptions, ExampleBigQueryTableOptions {
}