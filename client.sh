#!/bin/sh

mvn -e compile exec:java -Dexec.mainClass=dev.yoichiro.test.dataflow.MemoryToPubsub -Dexec.args="--toTopic=projects/$PROJECT_ID/topics/jobs --jobNum=1 --rowNum=1"
