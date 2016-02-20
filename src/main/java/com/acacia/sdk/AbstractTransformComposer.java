package com.acacia.sdk;


import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractTransformComposer extends DoFn<String,String> {

    public AbstractTransformComposer(){}

    public AbstractTransformComposer(Map<String, Object> args){};

    public abstract List<AbstractTransform> getOrderedTransforms();

    public Map<String,String> args;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTransform.class.getName());

    static final TupleTag<String> errorOutput = new TupleTag<String>("errorOutput"){};

    static final Gson gson = new Gson();

    @Override
    public final void processElement(ProcessContext processContext) {

        try {
            if (processContext != null && processContext.element() != null){

                String item = processContext.element();


                for(AbstractTransform transform : getOrderedTransforms()) {

                    if (args != null) {
                         item = transform.transform(item, args);
                    } else {
                         item = transform.transform(item);
                    }
                }

                if (item != null) {

                    processContext.output(item);
                }



            }
            else
            {
                throw new GenericDataflowAppException("null processcontext or element");
            }
        }
        catch(GenericDataflowAppException e){

            //deserialize json

            Map<String, Object> hm = gson.<Map<String, Object>>fromJson(
                    processContext.element(),
                    (new HashMap<String, Object>()).getClass());

            //add new error field, errordt
            hm.put("cause",e.getCause().getMessage());
            hm.put("errortest", "why");
            hm.put("error", e.getMessage());
            hm.put("errortimestamp", Long.toString(System.currentTimeMillis()));
            String s = gson.toJson(hm);

            LOG.debug("error: " + s);

            e.printStackTrace();



            processContext.sideOutput(errorOutput, s );
        }

    }



}
