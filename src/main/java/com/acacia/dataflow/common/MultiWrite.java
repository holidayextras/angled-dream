package com.acacia.dataflow.common;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MultiWrite  {
    public static Bound<String> named(String name) {
        return new Bound<>(PubsubIO.DEFAULT_PUBSUB_CODER).named(name);
    }

    /** The topic to publish to.
     * Cloud Pubsub topic names should be {@code /topics/<project>/<topic>},
     * where {@code <project>} is the name of the publishing project.
     */
    public static Bound<String> topics(List<String> topics) {
        return new Bound<>(PubsubIO.DEFAULT_PUBSUB_CODER).topics(topics);
    }

    /**
     * If specified, Dataflow will add a Pubsub label to each output record specifying the logical
     * timestamp of the record. {@code <timestampLabel>} determines the label name. The label value
     * is a numerical value representing the number of milliseconds since the Unix epoch. For
     * example, if using the joda time classes, the org.joda.time.Instant(long) constructor can be
     * used to parse this value. If the output from this sink is being read by another Dataflow
     * source, then PubsubIO.Read.timestampLabel can be used to ensure that the other source reads
     * these timestamps from the appropriate label.
     */
    public static Bound<String> timestampLabel(String timestampLabel) {
        return new Bound<>(PubsubIO.DEFAULT_PUBSUB_CODER).timestampLabel(timestampLabel);
    }

    /**
     * If specified, Dataflow will add a Pubsub label to each output record containing a unique
     * identifier for that record. {@code <idLabel>} determines the label name. The label value
     * is an opaque string value. This is useful if the the output from this sink is being read
     * by another Dataflow source, in which case PubsubIO.Read.idLabel can be used to ensure that
     * the other source reads these ids from the appropriate label.
     */
    public static Bound<String> idLabel(String idLabel) {
        return new Bound<>(PubsubIO.DEFAULT_PUBSUB_CODER).idLabel(idLabel);
    }

    /**
     * Returns a TextIO.Write PTransform that uses the given
     * {@code Coder<T>} to encode each of the elements of the input
     * {@code PCollection<T>} into an output PubSub record.
     *
     * <p>By default, uses {@link StringUtf8Coder}, which writes input
     * Java strings directly as records.
     *
     * @param <T> the type of the elements of the input PCollection
     */
    public static <T> Bound<T> withCoder(Coder<T> coder) {
        return new Bound<>(coder);
    }

    /**
     * A {@link PTransform} that writes an unbounded {@code PCollection<String>}
     * to a PubSub stream.
     */
    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
        /** The Pubsub topic to publish to. */
        List<PubsubIO.PubsubTopic> topics;
        String timestampLabel;
        String idLabel;
        final Coder<T> coder;

        Bound(Coder<T> coder) {
            this.coder = coder;
        }

        Bound(String name, List<PubsubIO.PubsubTopic> topics, String timestampLabel, String idLabel, Coder<T> coder) {
            super(name);
            if (topics != null) {
                this.topics = topics;
            }
            this.timestampLabel = timestampLabel;
            this.idLabel = idLabel;
            this.coder = coder;
        }

        /**
         * Returns a new PubsubIO.Write PTransform that's like this one but with the given step
         * name. Does not modify the object.
         */
        public Bound<T> named(String name) {
            return new Bound<>(name, topics, timestampLabel, idLabel, coder);
        }

        /**
         * Returns a new PubsubIO.Write PTransform that's like this one but writing to the given
         * topic. Does not modify the object.
         */
        public Bound<T> topics(List<String> topicList) {

            List<PubsubIO.PubsubTopic> pstopics = new ArrayList<>();
            for(String s : topicList)
            {
                pstopics.add(PubsubIO.PubsubTopic.fromPath(s));

            }

            return new Bound<>(name, pstopics, timestampLabel, idLabel, coder);
        }

        /**
         * Returns a new PubsubIO.Write PTransform that's like this one but publishing timestamps
         * to the given PubSub label. Does not modify the object.
         */
        public Bound<T> timestampLabel(String timestampLabel) {
            return new Bound<>(name, topics, timestampLabel, idLabel, coder);
        }

        /**
         * Returns a new PubsubIO.Write PTransform that's like this one but publishing record ids
         * to the given PubSub label. Does not modify the object.
         */
        public Bound<T> idLabel(String idLabel) {
            return new Bound<>(name, topics, timestampLabel, idLabel, coder);
        }

        /**
         * Returns a new PubsubIO.Write PTransform that's like this one
         * but that uses the given {@code Coder<X>} to encode each of
         * the elements of the input {@code PCollection<X>} into an
         * output record.  Does not modify this object.
         *
         * @param <X> the type of the elements of the input PCollection
         */
        public <X> Bound<X> withCoder(Coder<X> coder) {
            return new Bound<>(name, topics, timestampLabel, idLabel, coder);
        }

        @Override
        public PDone apply(PCollection<T> input) {
            if (topics == null) {
                throw new IllegalStateException(
                        "need to set the topic of a PubsubIO.Write transform");
            }
            input.apply(ParDo.of(new PubsubWriter()));
            return PDone.in(input.getPipeline());
        }

        @Override
        protected Coder<Void> getDefaultOutputCoder() {
            return VoidCoder.of();
        }

        public List<PubsubIO.PubsubTopic> getTopics() {
            return topics;
        }

        public String getTimestampLabel() {
            return timestampLabel;
        }

        public String getIdLabel() {
            return idLabel;
        }

        public Coder<T> getCoder() {
            return coder;
        }

        private class PubsubWriter extends DoFn<T, Void> {
            private static final int MAX_PUBLISH_BATCH_SIZE = 100;
            private transient List<PubsubMessage> output;
            private transient Pubsub pubsubClient;


            @Override
            public void startBundle(Context c) {
                this.output = new ArrayList<>();
                this.pubsubClient =
                        Transport.newPubsubClient(c.getPipelineOptions().as(DataflowPipelineOptions.class))
                                .build();
            }

            @Override
            public void processElement(ProcessContext c) throws IOException {
                PubsubMessage message = new PubsubMessage().encodeData(
                        CoderUtils.encodeToByteArray(getCoder(), c.element()));
                if (getTimestampLabel() != null) {
                    Map<String, String> attributes = message.getAttributes();
                    if (attributes == null) {
                        attributes = new HashMap<>();
                        message.setAttributes(attributes);
                    }
                    attributes.put(
                            getTimestampLabel(), String.valueOf(c.timestamp().getMillis()));
                }
                output.add(message);

                if (output.size() >= MAX_PUBLISH_BATCH_SIZE) {
                    publish();
                }
            }

            @Override
            public void finishBundle(Context c) throws IOException {
                if (!output.isEmpty()) {
                    publish();
                }
            }

            private void publish() throws IOException {
                PublishRequest publishRequest = new PublishRequest().setMessages(output);

                for(PubsubIO.PubsubTopic t : getTopics()){
                    pubsubClient.projects().topics()
                            .publish(t.asV1Beta2Path(), publishRequest).execute();
                }

                output.clear();
            }
        }
    }
}

