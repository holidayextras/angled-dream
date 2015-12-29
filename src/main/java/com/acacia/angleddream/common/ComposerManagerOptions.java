package com.acacia.angleddream.common;

import com.google.cloud.dataflow.sdk.options.BigQueryOptions;


/**
 * Options supported by pipelines.
 * <p>
 * <p>Inherits standard configuration options.
 */
public interface ComposerManagerOptions
        extends PubsubTopicOptions, PipelineComposerOptions, BigQueryOptions, BQTableOptions {
}