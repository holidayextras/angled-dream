package com.acacia.dataflow;


import clojure.lang.IFn;
import clojure.lang.RT;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.runners.DataflowPipeline;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLClassLoader;
import java.util.*;

public class OrionToBigQuery {


    static URLClassLoader classloader;
    static Gson gson = new Gson();

    private static final Logger LOG = LoggerFactory.getLogger(OrionToBigQuery.class);

    /**
     * Prepares the data for writing to BigQuery by building a TableRow object containing an
     * integer representation of month and the number of tornadoes that occurred in each month.
     */
    static class CreateBigquery extends DoFn<String, TableRow> {

        static IFn extract = null;

       static{
           try {
               RT.loadResourceScript("extract_orion.clj");
               extract = RT.var("extract-orion", "extract");
           } catch (IOException e) {
               e.printStackTrace();
           }

       }


        @Override
        public void processElement(ProcessContext c) {

            Map hm = (Map) extract.invoke(c.element());

            if (hm != null){

            LOG.warn(hm.getClass().getName());

            if(hm.size() > 2) {

                TableRow t = new TableRow();

                t.putAll(hm);
                try {
                    LOG.warn(t.toPrettyString());
                } catch (IOException e) {
                    e.printStackTrace();
                }

                c.output(t);
            }
else{ LOG.warn("null item");}

            }
        }
    }




    /**
     * Sets up and starts streaming pipeline.
     *
     * NOTE: needs to take some kind of exectution graph as an argument? paths to oooo
     *
     * @throws IOException if there is a problem setting up resources
     */
    public static void main(String[] args) throws IOException {


        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);

        // In order to cancel the pipelines automatically,
        // {@literal DataflowPipelineRunner} is forced to be used.
        options.setRunner(DataflowPipelineRunner.class);
        options.setProject("hx-test");
//        options.setMaxNumWorkers(1);
//        options.setNumWorkers(1);
        options.setWorkerMachineType("n1-standard-1");
        options.setZone("europe-west1-d");
        options.setStagingLocation("gs://hx-staging-alleu");
        options.setTempLocation("gs://hx-temp");

        Pipeline pipeline = DataflowPipeline.create(options);

                //create schema with bq update dataset:table schema.json


     //   pipeline.apply(TextIO.Read.named("ReadJSON").from("gs://hx-orion/production/archive/20150506/ec2-46-137-31-55.eu-west-1.compute.amazonaws.com.program.log.201505060000.gz"))
        pipeline.apply(TextIO.Read.named("ReadJSON").from("gs://hx-orion/production/archive/20150506/*"))

                .apply(ParDo.of(new CreateBigquery()))
                .apply(BigQueryIO.Write

                        .to("hx-test:hx_orion.hx_test")
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));



//        String s = "{\"browser\": {\"cookie_enabled\": \"True\",\n              \"inner_height\": 714,\n              \"inner_width\": 1440,\n              \"page_height\": 714,\n              \"user_agent\": \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.122 Safari/537.36\"},\n \"client_timestamp\": 1412942308097,\n \"date\": \"2014-10-10\",\n \"domain\": \"v2staging.holidayextras.co.uk\",\n \"email\": \"andrew.castle+testtest@holidayextras.com\",\n \"event\": \"Loaded a Page\",\n \"event_type\": \"availability\",\n \"is_client\": \"True\",\n \"orion_timestamp\": \"1412942100000\",\n \"orion_uuid\": \"09084093-a810-48f6-bcde-593de9ad5867\",\n \"orion_writetime\": 1412942307234,\n \"owts\": \"75b6f1752fd946ef75c3bc4b8ed9a47e\",\n \"owts_tmp\": \"e8b0ed6a67e0a4f10384421cf2163f9b\",\n \"page_id\": \"cda1dc16c226c1a4c576504f31e9c165\",\n \"path\": \"/carpark\",\n \"port\": \"\",\n \"product_type\": \"carpark\",\n \"protocol\": \"https\",\n \"referrer\": \"http://www.holidayextras.co.uk/\",\n \"request_params\": {\"arrival_date\": \"NaN-NaN-NaN\",\n                     \"arrival_time\": \"NaN:NaN:NaN\",\n                     \"customer_ref\": \"\",\n                     \"depart\": \"LGW\",\n                     \"depart_date\": \"2014-10-17\",\n                     \"depart_time\": \"01:00:00\",\n                     \"flight\": \"TBC\",\n                     \"park_to\": \"13:00\",\n                     \"rrive\": \"\",\n                     \"term\": \"\",\n                     \"terminal\": \"\"},\n \"screen\": {\"height\": 900, \"pixelDepth\": 24, \"width\": 1440},\n \"server_ip\": \"10.210.175.195\",\n \"service\": \"webapp\",\n \"source_ip\": \"62.254.236.250\",\n \"time\": \"12:58:28\",\n \"title\": \"Parking 17 October 2014 | HolidayExtras.com\",\n \"url\": \"https://v2staging.holidayextras.co.uk/?agent=WEB1&ppcmsg=\"}";



        //System.out.println(t.toPrettyString());
        PipelineResult result = pipeline.run();

    }


}
