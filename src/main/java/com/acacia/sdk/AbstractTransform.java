package com.acacia.sdk;


import com.acacia.angleddream.common.Tags;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public abstract class AbstractTransform extends DoFn<String,String> {


    private static final Logger LOG = LoggerFactory.getLogger(AbstractTransform.class.getName());

    static final TupleTag<String> errorOutput = new TupleTag<String>("errorOutput"){};

    //function to transform a string, used by processElement

    public Map<String,String> args;

    public String transform(String input) throws GenericDataflowAppException {return input;}



    public String transform(String input, Map<String,String> args) throws GenericDataflowAppException {return input;}


    static final Gson gson = new Gson();


    @Override
    public final void processElement(ProcessContext processContext) {

        try {
            if(args != null)
            {
                processContext.output(transform(processContext.element(), args));
            }
            else {
                processContext.output(transform(processContext.element()));
            }
        }
        catch(Exception e){

            //deserialize json

            Map<String, Object> hm = gson.<Map<String, Object>>fromJson(
                    processContext.element(),
                    (new HashMap<String, Object>()).getClass());

            //add new error field, errordt
            hm.put("error", e.getMessage());
            hm.put("errortimestamp", Long.toString(System.currentTimeMillis()));
            String s = gson.toJson(hm);

            LOG.debug(s);
            System.out.println("exception log: " + s);

            processContext.sideOutput(errorOutput, s );
        }

    }
}
