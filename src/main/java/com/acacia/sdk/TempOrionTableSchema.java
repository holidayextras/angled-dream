package com.acacia.sdk;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.List;

public class TempOrionTableSchema {

    TableSchema ts;


    public TempOrionTableSchema(){

        List<TableFieldSchema> fields = new ArrayList<>();

        fields.add(new TableFieldSchema().setName("event").setType("STRING").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("owts").setType("STRING").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("owts_tmp_v2").setType("STRING").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("page_id").setType("STRING").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("page_action_id").setType("STRING").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("date").setType("STRING").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("timestamp").setType("timestamp").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("orion_writetime").setType("timestamp").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("client_timestamp").setType("timestamp").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("protocol").setType("STRING").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("domain").setType("STRING").setMode("nullable"));
        fields.add(new TableFieldSchema().setName("path").setType("STRING").setMode("nullable"));



        List<TableFieldSchema> dataList = new ArrayList<>();
        TableFieldSchema data = new TableFieldSchema().setName("data").setType("record").setMode("nullable");

         List<TableFieldSchema> dataActionList = new ArrayList<>();
         TableFieldSchema dataAction = new TableFieldSchema().setName("action").setType("record").setMode("nullable");

          dataActionList.add(new TableFieldSchema().setName("name").setType("STRING").setMode("nullable"));
          dataAction.setFields(dataActionList);

          dataList.add(dataAction);

         dataList.add(new TableFieldSchema().setName("test_name").setType("STRING").setMode("nullable"));
         dataList.add(new TableFieldSchema().setName("variant").setType("STRING").setMode("nullable"));
         dataList.add(new TableFieldSchema().setName("step").setType("STRING").setMode("nullable"));
         dataList.add(new TableFieldSchema().setName("booking_ref").setType("STRING").setMode("nullable"));

         TableFieldSchema dataResourceParams = new TableFieldSchema().setName("resource_params").setType("record").setMode("nullable");
         List<TableFieldSchema> dataResourceParamsList = new ArrayList<>();
         dataResourceParamsList.add(new TableFieldSchema().setName("bkg_ref").setType("STRING").setMode("nullable"));
         dataResourceParamsList.add(new TableFieldSchema().setName("bref").setType("STRING").setMode("nullable"));
         dataResourceParamsList.add(new TableFieldSchema().setName("bkref").setType("STRING").setMode("nullable"));

        dataResourceParams.setFields(dataResourceParamsList);
        data.setFields(dataList);

        List<TableFieldSchema> screenList = new ArrayList<>();
        TableFieldSchema screen = new TableFieldSchema().setName("screen").setType("record").setMode("nullable");

        screenList.add(new TableFieldSchema().setName("height").setType("integer").setMode("nullable"));
        screenList.add(new TableFieldSchema().setName("width").setType("integer").setMode("nullable"));

        screen.setFields(screenList);
        fields.add(screen);

        fields.add(new TableFieldSchema().setName("source_ip").setType("STRING").setMode("nullable"));


        TableSchema schema = new TableSchema().setFields(fields);
       ts = schema;


    }

    public TableSchema getOrionTS(){

        return ts;
    }

}
