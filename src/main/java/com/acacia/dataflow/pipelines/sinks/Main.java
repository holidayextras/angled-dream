package com.acacia.dataflow.pipelines.sinks;

import com.acacia.dataflow.common.*;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipeline;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator;
import com.google.cloud.dataflow.sdk.runners.dataflow.TextIOTranslator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class Main {


    private final static long DEFAULT_READ_BUFFER = 60L * 1000L;

    public static void main(String[] args) throws IOException {

        PubsubTopicOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PubsubTopicOptions.class);
        options.setStreaming(true);
        options.setRunner(DataflowPipelineRunner.class);
        options.setMaxNumWorkers(1);
        options.setNumWorkers(1);
        options.setWorkerMachineType("n1-standard-1");
        options.setJobName("OutputToGS-9");


        Pipeline pipeline = Pipeline.create(options);


        pipeline.apply("Read",PubsubIO.Read.topic(options.getPubsubTopic()).maxReadTime(new Duration(DEFAULT_READ_BUFFER)))
                .apply(ParDo.named("DoNothing").of(new DoFn<String, String>() {
                    @Override
                    public void processElement(ProcessContext c) {
                                c.output(c.element());
                        }
                    }
                ))
                .apply("Write", SinkFactory.getGoogleCloudOutput("gs://hx-test/first-dataflow-out"));


        PipelineResult result = pipeline.run();

    }

}

