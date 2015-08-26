package com.nvent.storm.perftest;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class PerfTestConfig {
  @Parameter(names = "--zk-connect", description = "Zookeeper connect")
  public String zkConnect    = "localhost:2181";
  
  @Parameter(names = "--kafka-connect", description = "Kafka connect")
  public String kafkaConnect = "localhost:9092" ;
  
  @Parameter(names = "--topic-input", description = "Topic Input")
  public String topicIn      =  "perftest.in";
  
  @Parameter(names = "--topic-output", description = "Topic Output")
  public String topicOut     =  "perftest.out";
  
  @Parameter(names = "--num-of-partition", description = "Num Of Partition")
  public int numOPartition  =  2;
  
  @Parameter(names = "--num-of-message-per-partition", description = "Num Of Message Per Partition")
  public int numOfMessagePerPartition  =  25000;
  
  @Parameter(names = "--message-size", description = "Message Size")
  public int messageSize  =  128;
  
  @Parameter(names = "--storm-nimbus-host", description = "Storm Nimbus Host")
  public String stormNimbusHost ;
  
  @Parameter(names = "--storm-jar-files", description = "Storm Jar Files")
  public String stormJarFiles ;
  
  @Parameter(names = "--storm-topology-name", description = "Storm Jar Files")
  public String stormTopologyName = "PerfTest";
  
  public PerfTestConfig(String[] args) {
    new JCommander(this, args);
  }
}
