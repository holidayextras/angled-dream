# angled-dream

PubSub -> Cloud Dataflow Pipeline Composition.


1. Build angleddream (you shouldn't need to do this, it's continuously built in a Maven repo):
    1. git clone https://github.com/22Acacia/angled-dream
    1. cd angled-dream
    1. mvn package


## To execute by itself (terraform takes care of this in prod)


1. Make sure you are authed into gcloud with the correct project
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


VERSIONS.txt
this file is a listing of the last built jar files as found in the target folder.  This file is then uploaded
to google storage to a known bucket and then consuming application can chose which jar file to download. 

### Deployment

This is deployed using https://github.com/aktau/github-release/, CircleCI, and https://jitpack.io/
