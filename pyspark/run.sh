#!/usr/bin/bash
spark-submit \
  --jars $SPARK_HOME/jars/scala-logging-api_2.11-2.1.2.jar,$SPARK_HOME/jars/scala-logging-slf4j_2.11-2.1.2.jar \
  --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 \
  --master local app.py
