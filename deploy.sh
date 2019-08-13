#!/bin/sh

mvn compile exec:java -Dexec.mainClass=dev.yoichiro.test.dataflow.PubsubToDatastore -Dexec.args="--project=$PROJECT_ID --stagingLocation=gs://yoichiro-dataflow-test/staging --autoscalingAlgorithm=NONE --numWorkers=1 --fromTopic=projects/$PROJECT_ID/topics/jobs --toNamespace= --toKind=jobs --toNamePrefix=df --runner=DataflowRunner"
