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

JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1536m -XX:-UseSplitVerifier" 
LOG_OPT="-Dlog4j.configuration=$APP_DIR/conf/log4j.properties"

JAR_FILES="$APP_DIR/libs/module.storm-all-1.0-SNAPSHOT.jar"

MAIN_CLASS="com.nvent.storm.perftest.PerfTest"
$JAVACMD -Djava.ext.dirs=$APP_DIR/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $LOG_OPT $MAIN_CLASS \
  --zk-connect zookeeper-1:2181\
  --kafka-connect kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --num-of-partition 2 \
  --num-of-message-per-partition 50000 \
  --message-size 512 \
  --storm-nimbus-host storm-nimbus --storm-jar-files $JAR_FILES --storm-topology-name PerfTest
