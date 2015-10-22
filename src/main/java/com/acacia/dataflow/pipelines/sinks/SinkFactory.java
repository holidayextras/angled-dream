package com.acacia.dataflow.pipelines.sinks;


import com.google.cloud.dataflow.sdk.io.TextIO;

public class SinkFactory {

    public static StreamTextIO.Write.Bound<String> getGoogleCloudOutput(String destination){
        return StreamTextIO.Write.to(destination);
    }

}
