package com.acacia.sdk;


import com.google.cloud.dataflow.sdk.transforms.DoFn;


public abstract class AbstractTransform extends DoFn<String,String> {


    //function to transform a string, used by processElement
    abstract public String transform(String input);


    @Override
    public final void processElement(ProcessContext processContext) throws Exception {
        processContext.output(transform(processContext.element()));
    }
}
