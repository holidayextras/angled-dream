# angled-dream
PubSub -> Cloud Dataflow Pipeline Orchestration

NOTE: need to write a script to manually build classpath so we can include all the component jars people build

Set Environment Variables: AWS_ACCESS_KEY_ID + AWS_SECRET_KEY to deploy builds

Standing up a bigtable:

Bradfords-MacBook-Pro:~ bradfordstephens$ bq rm -f hx_orion.hx_test
Bradfords-MacBook-Pro:~ bradfordstephens$ bq --dataset_id=hx_orion  mk hx_test
Table 'hx-test:hx_orion.hx_test' successfully created.
Bradfords-MacBook-Pro:~ bradfordstephens$ bq update hx_orion.hx_test ~/proj/angled-dream/src/orion-test-schema.json
Table 'hx-test:hx_orion.hx_test' successfully updated.