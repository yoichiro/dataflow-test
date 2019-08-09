# Cloud Dataflow Test Project

This is a test project to use Dataflow with Pub/Sub and Datastore.

## Prerequisite

* You need to create a topic on Pub/Sub.
* You need to create a kind on Datastore.
* You need to issue and download a security key file and to specify its path to `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

## How to use

**Server**

```
mvn compile exec:java \
-Dexec.mainClass=dev.yoichiro.test.dataflow.PubsubToDatastore \
-Dexec.args="--project=dataflow-test-249303 \ 
--stagingLocation=gs://yoichiro-dataflow-test/staging \
--autoscalingAlgorithm=NONE \
--numWorkers=1 \
--fromTopic=projects/dataflow-test-249303/topics/jobs \
--toNamespace= \
--toKind=jobs \
--toNamePrefix=df \
--runner=DataflowRunner"
```

**Client**

```
mvn -e compile exec:java \
-Dexec.mainClass=dev.yoichiro.test.dataflow.MemoryToPubsub \
-Dexec.args="--toTopic=projects/dataflow-test-249303/topics/jobs \
--jobNum=100 \
--rowNum=100"
```
