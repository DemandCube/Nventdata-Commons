#!/bin/bash

cygwin=false
ismac=false
case "`uname`" in
  CYGWIN*) cygwin=true;;
  Darwin) ismac=true;;
esac

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

export HADOOP_USER_NAME="neverwinterdp"

APP_DIR=`cd $bin/..; pwd; cd $bin`
JAVACMD=$JAVA_HOME/bin/java

if [ "x$JAVA_HOME" == "x" ] ; then 
  echo "WARNING JAVA_HOME is not set"
fi

(which $JAVACMD)
isjava=$?

if $ismac && [ $isjava -ne 0 ] ; then
  which java
  if [ $? -eq 0 ] ; then
    JAVACMD=`which java`
    echo "Defaulting to java: $JAVACMD"
  else 
    echo "JAVA Command (java) Not Found Exiting"
    exit 1
  fi
fi

FLINK_HOME="/opt/flink" 
JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1536m -XX:-UseSplitVerifier" 
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"

MAIN_CLASS="com.nvent.flink.perftest.PerfTest"
$JAVACMD -Djava.ext.dirs=$APP_DIR/libs:$FLINK_HOME/lib:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $APP_OPT $LOG_OPT $MAIN_CLASS \
  --zk-connect zookeeper-1:2181\
  --kafka-connect kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --num-of-partition 2 \
  --num-of-message-per-partition 25000 \
  --message-size 512 \
  --output-path hdfs://hadoop-master:9000/tmp/perftest
