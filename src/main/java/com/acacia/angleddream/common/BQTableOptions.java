package com.acacia.angleddream.common;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;


public interface BQTableOptions extends PipelineOptions {


    @Description("BigQuery dataset name")
    String getBigQueryDataset();
    void setBigQueryDataset(String dataset);

    @Description("BigQuery table name")
    String getBigQueryTable();
    void setBigQueryTable(String table);

    @Description("BigQuery table name")
    String getBigQuerySchemaFile();
    void setBigQuerySchemaFile(String table);

//    @Description("BigQuery table schema")
//    TableSchema getBigQuerySchema();
//    void setBigQuerySchema(TableSchema schema);

}
