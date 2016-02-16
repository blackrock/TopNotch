#!/bin/bash

export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}

function setUploadFiles { local IFS=","; uploadFiles="$*"; }
setUploadFiles $@
launchDirectory=$(pwd)
scriptDirectory=`dirname "${BASH_SOURCE-$0}"`

: ${TOPNOTCH_JAR:="$scriptDirectory"/topnotch-assembly-1.0.jar}
: ${MNO:=100}
: ${MASTER:=yarn-cluster}
: ${MAIN:=com.bfm.topnotch.tnengine.TnEngine}

cd "$scriptDirectory"

if [ ! -f jq-linux64 ]; then
  wget https://github.com/stedolan/jq/releases/download/jq-1.5/jq-linux64
  chmod a+x jq-linux64
fi

cd "$launchDirectory"

for i in $@; do
  echo File: $i
  cat $i | "$scriptDirectory"/jq-linux64
done


if [ $# -eq 0 ]
then
  MASTER="yarn-client"
  PARAMS="--help"
  exec $SPARK_HOME/bin/spark-submit \
    --master $MASTER \
    --class $MAIN \
    --num-executors $MNO \
    $TOPNOTCH_JAR
else
  exec $SPARK_HOME/bin/spark-submit \
    --master $MASTER \
    --class $MAIN \
    --num-executors $MNO \
    --driver-java-options -XX:MaxPermSize=512m \
    --driver-memory=3g \
    --executor-memory=3g \
    --files $uploadFiles \
    $TOPNOTCH_JAR \
    $(basename $1)
fi
