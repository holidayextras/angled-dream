package com.acacia.dataflow.common;

import com.acacia.dataflow.pipelines.sinks.Bound2;
import com.google.cloud.dataflow.sdk.io.ShardNameTemplate;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator;
import com.google.cloud.dataflow.sdk.runners.dataflow.TextIOTranslator;
import com.google.cloud.dataflow.sdk.util.PathValidator;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.common.base.Preconditions;

public class HateWriteTranslator implements DataflowPipelineTranslator.TransformTranslator<Bound2.Write.Bound> {

    @Override
    public void translate(
            Bound2.Write.Bound transform,
            DataflowPipelineTranslator.TranslationContext context) {
        translateWriteHelper(transform, context);
    }



//  super fuck this

    private <T> void translateWriteHelper(
            Bound2.Write.Bound<T> transform,
            DataflowPipelineTranslator.TranslationContext context) {
//        if (context.getPipelineOptions().isStreaming()) {
//            throw new IllegalArgumentException("TextIO not supported in streaming mode.");
//        }

        PathValidator validator = context.getPipelineOptions().getPathValidator();
        String filenamePrefix = validator.validateOutputFilePrefixSupported(
                transform.getFilenamePrefix());

        context.addStep(transform, "ParallelWrite");
        context.addInput(PropertyNames.PARALLEL_INPUT, context.getInput(transform));

        // TODO: drop this check when server supports alternative templates.
        switch (transform.getShardTemplate()) {
            case ShardNameTemplate.INDEX_OF_MAX:
                break;  // supported by server
            case "":
                // Empty shard template allowed - forces single output.
                Preconditions.checkArgument(transform.getNumShards() <= 1,
                        "Num shards must be <= 1 when using an empty sharding template");
                break;
            default:
                throw new UnsupportedOperationException("Shard template "
                        + transform.getShardTemplate()
                        + " not yet supported by Dataflow service");
        }

        // TODO: How do we want to specify format and
        // format-specific properties?
        context.addInput(PropertyNames.FORMAT, "text");
        context.addInput(PropertyNames.FILENAME_PREFIX, filenamePrefix);
        context.addInput(PropertyNames.SHARD_NAME_TEMPLATE,
                transform.getShardNameTemplate());
        context.addInput(PropertyNames.FILENAME_SUFFIX, transform.getFilenameSuffix());
        context.addInput(PropertyNames.VALIDATE_SINK, transform.needsValidation());

        long numShards = transform.getNumShards();
        if (numShards > 0) {
            context.addInput(PropertyNames.NUM_SHARDS, numShards);
        }

        context.addEncodingInput(
                WindowedValue.getValueOnlyCoder(transform.getCoder()));
    }

}
