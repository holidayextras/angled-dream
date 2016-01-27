package com.acacia.angleddream.common;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BigQueryProcessor extends DoFn<String, TableRow> {

    static final Gson gson = new Gson();

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryProcessor.class);

    @Override
    public void processElement(ProcessContext c) {


        //turn input string into json object for consumption by bq

//        System.out.println("BigQuery input:" + c.element());


        Map<String, Object> hm = gson.<Map<String, Object>>fromJson(
                c.element(),
                (new HashMap<String, Object>()).getClass());



        if (hm != null){

            if(hm.size() > 0) {

                TableRow t = new TableRow();
                t.putAll(hm);
                c.output(t);
            }

        }
    }


}
