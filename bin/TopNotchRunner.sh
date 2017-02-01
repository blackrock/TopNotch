#!/bin/bash

# uncomment the below lines if you want to set SPARK_HOME and HADOOP_CONF_DIR in this script
# export SPARK_HOME=${SPARK_HOME:-LOCATION_SPARK_HOME}
# export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-LOCATION_HADOOP_HOME}

scriptDirectory=`dirname "${BASH_SOURCE-$0}"`
: ${TOPNOTCH_JAR:="$scriptDirectory"/TopNotch-assembly-0.2.jar}
: ${MASTER:=local}
: ${MAIN:=com.bfm.topnotch.tnengine.TnEngine}

$SPARK_HOME/bin/spark-submit \
--master $MASTER \
--class $MAIN \
--conf "spark.yarn.executor.memoryOverhead=1000" \
$TOPNOTCH_JAR \
$@
