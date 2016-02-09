# angled-dream

PubSub -> Cloud Dataflow Pipeline Composition.

Angled-Dream takes .jar files containing classes which implement AbstractTransformComposer and AbstractTransform, turns them into Dataflow jobs, sets up inputs and outputs, and executes them.


Maven:

    <repositories>
        <repository>
            <id>jitpack.io</id>
            <url>https://jitpack.io</url>
        </repository>
    </repositories>

    <dependencies>
        <dependency>
            <groupId>com.github.22Acacia</groupId>
            <artifactId>angled-dream</artifactId>
            <version>-SNAPSHOT</version>
        </dependency>
    </dependencies>

1. Build angleddream (you shouldn't need to do this, it's continuously built in a Maven repo):
    1. git clone https://github.com/22Acacia/angled-dream
    1. cd angled-dream
    1. mvn package


## To execute by itself (You should not do this unless you need to manually test something in the cloud -- Sossity takes care of all deployment)


1. Make sure you are autheticated into gcloud with the correct project
1. Make sure you have built a standalone .jar for your pipeline with `mvn package`
2. Execute dataflow job according to below:

Command line options:

To avoid billing/deployment mishaps, there are no defaults. Flags not provided will throw an exception and exit.

    java -classpath angleddream-bundled-0.1-ALPHA.jar:/home/proj/examples/target/transformexamples-0.1-ALPHA-bundled.jar com.acacia.angleddream.Main --stagingLocation=gs://hx-test/staging --project=hx-test --pubsubTopic=projects/hx-test/topics/data-topic --outputTopics=projects/hx-test/topics/output1,projects/hx-test/topics/output2 --maxNumWorkers=1 --numWorkers=1 --zone=europe-west1-c --workerMachineType=n1-standard-1 --errorPipelineName=projects/hx-test/topics/output2-to-error-output2

    -classpath: colon-separated jar files for angleddream and all other dependency jars (only one dash!)
    
    com.acacia.angleddream.Main: main class for Dataflow job. Does not change.
    
    --stagingLocation: google cloud storage bucket to upload jars and dependencies to
    --project: GCloud project name
    --pubsubTopic: input topic for dataflow job
    --outputTopics: comma-separated pubsubs to output results to
    --maxNumWorkers: maximum number of workers on a job
    --numWorkers: initial number of workers on a job
    --zone: GCE zone where workers reside
    --workerMachineType: instance type
    --errorPipelineName: pubsub topic to write errors to


Only for BigQuery

    --bigQueryDataset: destination Dataset for BigQuery sink
    --bigQueryTable: destination table for BigQuery sink
    --bigQuerySchema: path to JSON file describing BigQuery schema


### Deployment

This is deployed using https://github.com/aktau/github-release/, CircleCI, and https://jitpack.io/
