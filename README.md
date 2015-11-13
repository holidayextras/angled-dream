# angled-dream
PubSub -> Cloud Dataflow Pipeline Composition


TO RUN SAMPLES:

0. Make sure you are authed into gcloud with the correct project
1. Build angleddream:
 a. git clone https://github.com/22Acacia/angled-dream
 b. cd angled-dream
 c. mvn install
 d. mvn package
1. Build your operations into jars, sample:
 a. git clone https://github.com/22Acacia/pipeline-examples  (requires angleddream installed, will move to maven repo later)
 b. in pipeline-examples: mvn package
2. Execute dataflow job according to below:

Command line options:


java -classpath angleddream-bundled-0.1-ALPHA.jar:/home/bradford/proj/example-scaffolding/target/transformexamples-0.1-ALPHA.jar com.acacia.dataflow.Main --stagingLocation=gs://hx-test/staging --project=hx-test --pubsubTopic=projects/hx-test/topics/data-topic --outputTopics=projects/hx-test/topics/output1,projects/hx-test/topics/output2 --maxNumWorkers=1 --numWorkers=1 --zone=europe-west1-c --workerMachineType=n1-standard-1

-classpath: colon-separated jar file for angleddream and all other dependency jars (only one dash!)
com.acacia.dataflow.Main: main class for Dataflow job. usually this if you're using our sample libraries

--stagingLocation: GS3 bucket to upload jars and dependencies to
--project: GCloud project name
--pubsubTopic: input topic for dataflow job
--outputTopics: comma-separated pubsubs to output results to
--maxNumWorkers: maximum number of workers on a job
--numWorkers: initial number of workers on a job
--zone: GCE zone where workers reside
--workerMachineType: instance type


*** not implemented yet ***
--bigQueryDataset
--bigQueryTable
--bigQuerySchema??




