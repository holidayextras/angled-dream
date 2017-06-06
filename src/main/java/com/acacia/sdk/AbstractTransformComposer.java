package com.acacia.sdk;


import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.codec.digest.DigestUtils;

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

    static final Gson gson = new GsonBuilder().serializeNulls().create();

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

                if (item != null && !item.equals("")) {

                    //rehash


                    Map<String, Object> hm = gson.<Map<String, Object>>fromJson(item, (new HashMap<String, Object>()).getClass());


                    Object resource =  hm.get("resource");


                    if(resource instanceof List ){

                        List<Map<String,Object>> items = (List<Map<String,Object>>) resource;
                        for(Map<String, Object> m : items){

                            Map<String, Object> out = new HashMap<>();
                            out.putAll(hm);

                            out.put("resource_hash", DigestUtils.md5Hex(gson.toJson(m)));
                            out.put("resource", m);
                            String output = gson.toJson(out);
                            LOG.info("item output: " + output);
                            processContext.output(output);

                        }

                    }
                    else
                    {

                        hm.put("resource_hash", DigestUtils.md5Hex(gson.toJson(resource)));
                        String output = gson.toJson(hm);
                        LOG.info("item output: " + output);
                        processContext.output(output);

                    }



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
            hm.put("error", e.getMessage());
            hm.put("errortimestamp", Long.toString(System.currentTimeMillis()));
            String s = gson.toJson(hm);

            LOG.info("error: " + s);

            e.printStackTrace();
            processContext.sideOutput(errorOutput, s );
        }

    }



}
