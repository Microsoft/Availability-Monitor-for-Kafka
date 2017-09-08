#!/bin/bash
export JAVA_HOME=/opt/java/jdk1.8.0

export PATH=$PATH:$JAVA_HOME/bin

exec java -classpath /usr/bin/sphn/kat-service/kafka-availability-monitor/kafkaavailability-1.0-SNAPSHOT-jar-with-dependencies.jar \
            com.microsoft.kafkaavailability.App