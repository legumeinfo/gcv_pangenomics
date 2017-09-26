#!/usr/bin/bash
pyspark \
  --jars $SPARK_HOME/jars/scala-logging-api_2.11-2.1.2.jar,$SPARK_HOME/jars/scala-logging-slf4j_2.11-2.1.2.jar \
  --packages graphframes:graphframes:0.4.0-spark2.1-s_2.11
