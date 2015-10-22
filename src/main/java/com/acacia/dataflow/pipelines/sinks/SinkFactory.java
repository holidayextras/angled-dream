package com.acacia.dataflow.pipelines.sinks;


import com.google.cloud.dataflow.sdk.io.TextIO;

public class SinkFactory {

    public static Bound2.Write.Bound<String> getGoogleCloudOutput(String destination){
        return Bound2.Write.to("gs://hx-test/test-pubsub-output");
    }


}
