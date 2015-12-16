package com.acacia.angleddream.common;


import com.google.cloud.dataflow.sdk.options.*;

public interface PipelineComposerOptions extends PipelineOptions{

    @Description("Comma-delimited PubSub topics to output to")
    String getOutputTopics();
    void setOutputTopics(String outputTopics);

    @Description("Comma-delimited classnames (packaged in this jar) to execute as part of pipeline")
    String getExecutionPipelineClasses();
    void setExecutionPipelineClasses(String pipelineClasses);

    @Description("Comma-delimited path to files to get classes from for execution")
    String getExternalFiles();
    void setExternalFiles(String pipelineClasses);

    @Description("Pipeline Name")
    @Default.String("unnamed-pipeline")
    String getPipelineName();
    void setPipelineName(String pipelineName);


    @Description("Error Pipeline Name")
    @Default.String("error-unnamed-pipeline")
    String getErrorPipelineName();
    void setErrorPipelineName(String pipelineName);




}
