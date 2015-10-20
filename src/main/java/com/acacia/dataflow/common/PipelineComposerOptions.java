package com.acacia.dataflow.common;


import com.google.cloud.dataflow.sdk.options.*;

public interface PipelineComposerOptions extends PipelineOptions{

    @Description("Comma-delimited PubSub topics to output to")
    @Validation.Required
    String getOutputTopics();
    void setOutputTopics(String outputTopics);

    @Description("Comma-delimited classnames (packaged in this jar) to execute as part of pipeline")
    @Validation.Required
    String getExecutionPipelineClasses();
    void setExecutionPipelineClasses(String pipelineClasses);


    @Description("Comma-delimited path to files to get classes from for execution")
    @Validation.Required
    String getExternalFiles();
    void setExternalFiles(String pipelineClasses);




}
