package com.acacia.dataflow.pipelines.sinks;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.ShardNameTemplate;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.worker.TextSink;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;


public class Bound2 {


    public static final Coder<String> DEFAULT_TEXT_CODER = StringUtf8Coder.of();

    /**
     * Returns a TextIO.Write PTransform that writes to the file(s)
     * with the given prefix.  This can be a local filename
     * (if running locally), or a Google Cloud Storage filename of
     * the form {@code "gs://<bucket>/<filepath>"})
     * (if running locally or via the Google Cloud Dataflow service).
     * <p>
     * <p>The files written will begin with this prefix, followed by
     * a shard identifier (see {@link Bound#withNumShards}, and end
     * in a common extension, if given by {@link Bound#withSuffix}.
     */

    public static class Write {

        public static Bound<String> to(String prefix) {
            return new Bound<>(DEFAULT_TEXT_CODER).to(prefix);
        }


        public static class Bound<T> extends PTransform<PCollection<T>, PDone> {

            /**
             * The filename to write to.
             */
            @Nullable
            final String filenamePrefix;
            /**
             * Suffix to use for each filename.
             */
            final String filenameSuffix;

            /**
             * The Coder to use to decode each line.
             */
            final Coder<T> coder;

            /**
             * Requested number of shards.  0 for automatic.
             */
            final int numShards;

            /**
             * Shard template string.
             */
            final String shardTemplate;

            /**
             * An option to indicate if output validation is desired. Default is true.
             */
            final boolean validate;


            Bound(Coder<T> coder) {
                this(null, null, "", coder, 0, ShardNameTemplate.INDEX_OF_MAX, true);
            }

            Bound(String name, String filenamePrefix, String filenameSuffix, Coder<T> coder,
                  int numShards, String shardTemplate, boolean validate) {
                super(name);
                this.coder = coder;
                this.filenamePrefix = filenamePrefix;
                this.filenameSuffix = filenameSuffix;
                this.numShards = numShards;
                this.shardTemplate = shardTemplate;
                this.validate = validate;
            }


            /**
             * Returns a new TextIO.Write PTransform that's like this one but
             * that writes to the file(s) with the given filename prefix.
             * <p>
             * <p>See {@link Write#to(String) Write.to(String)} for more information.
             * <p>
             * <p>Does not modify this object.
             */
            public Bound<T> to(String filenamePrefix) {
                validateOutputComponent(filenamePrefix);
                return new Bound<>(name, filenamePrefix, filenameSuffix, coder, numShards, shardTemplate,
                        validate);
            }


            public String getFilenamePrefix() {
                return filenamePrefix;
            }

            public String getShardTemplate() {
                return shardTemplate;
            }

            public int getNumShards() {
                return numShards;
            }

            public String getFilenameSuffix() {
                return filenameSuffix;
            }

            public Coder<T> getCoder() {
                return coder;
            }

            @Override
            public PDone apply(PCollection<T> input) {
                if (filenamePrefix == null) {
                    throw new IllegalStateException(
                            "need to set the filename prefix of a TextIO.Write transform");
                }
                return PDone.in(input.getPipeline());
            }

            /**
             * Returns the current shard name template string.
             */
            public String getShardNameTemplate() {
                return shardTemplate;
            }

            @Override
            protected Coder<Void> getDefaultOutputCoder() {
                return VoidCoder.of();
            }

            static {
                DirectPipelineRunner.registerDefaultTransformEvaluator(
                        Bound.class, new DirectPipelineRunner.TransformEvaluator<Bound>() {
                            @Override
                            public void evaluate(
                                    Bound transform, DirectPipelineRunner.EvaluationContext context) {
                                evaluateWriteHelper(transform, context);
                            }
                        });
            }


            public boolean needsValidation() {
                return validate;
            }

        }


        // Pattern which matches old-style shard output patterns, which are now
        // disallowed.
        private static final Pattern SHARD_OUTPUT_PATTERN = Pattern.compile("@([0-9]+|\\*)");

        private static void validateOutputComponent(String partialFilePattern) {
            Preconditions.checkArgument(
                    !SHARD_OUTPUT_PATTERN.matcher(partialFilePattern).find(),
                    "Output name components are not allowed to contain @* or @N patterns: "
                            + partialFilePattern);
        }

    }


    private static <T> void evaluateWriteHelper(
            Write.Bound<T> transform, DirectPipelineRunner.EvaluationContext context) {
        List<T> elems = context.getPCollection(context.getInput(transform));
        int numShards = transform.numShards;
        if (numShards < 1) {
            // System gets to choose.  For direct mode, choose 1.
            numShards = 1;
        }
        TextSink<WindowedValue<T>> writer = TextSink.createForDirectPipelineRunner(
                transform.filenamePrefix, transform.getShardNameTemplate(), transform.filenameSuffix,
                numShards, true, null, null, transform.coder);
        try (Sink.SinkWriter<WindowedValue<T>> sink = writer.writer()) {
            for (T elem : elems) {
                sink.add(WindowedValue.valueInGlobalWindow(elem));
            }
        } catch (IOException exn) {
            throw new RuntimeException(
                    "unable to write to output file \"" + transform.filenamePrefix + "\"", exn);
        }
    }


}
