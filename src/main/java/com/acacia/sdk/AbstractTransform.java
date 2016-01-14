package com.acacia.sdk;


import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;


public abstract class AbstractTransform extends DoFn<String,String> {


    static final TupleTag<String> errorOutput = new TupleTag<String>("errorOutput"){};

    //function to transform a string, used by processElement
    abstract public String transform(String input) throws GenericDataflowAppException;

    static final Gson gson = new Gson();


    @Override
    public final void processElement(ProcessContext processContext) {

        try {
            processContext.output(transform(processContext.element()));
        }
        catch(Exception e){

            //deserialize json

            Map<String, Object> hm = gson.<Map<String, Object>>fromJson(
                    processContext.element(),
                    (new HashMap<String, Object>()).getClass());

            //add new error field, errordt
            hm.put("error", e.getMessage());
            hm.put("errordt", Long.toString(System.currentTimeMillis()));
            String s = gson.toJson(hm);

            processContext.sideOutput(errorOutput, s );
        }

    }
}
