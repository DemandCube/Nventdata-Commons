package com.nvent.storm.perftest;

import com.nvent.tool.message.Message;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class PerfTest {
  final static public String IN_TOPIC  = "perftest.in";
  final static public String OUT_TOPIC = "perftest.out";
  
  private String zkConnect    = "127.0.0.1:2181";
  private String kafkaConnect = "127.0.0.1:9092";
  private String inTopic      =  IN_TOPIC;
  private String outTopic     =  OUT_TOPIC;
  
  public PerfTest(String[] args) {
  }
  
  private TopologyBuilder createTopologyBuilder() {
    TopologyBuilder builder = new TopologyBuilder();
    KafkaTopicSpout<Message> messageSpout = 
        new KafkaTopicSpout<Message>("PerfTestSpout", zkConnect, inTopic, Message.class);
    builder.setSpout("spout", messageSpout, 5);
    builder.setBolt("split",  new SplitStream(), 5).shuffleGrouping("spout");
    
    SaveStreamToKafka saveP0 = new SaveStreamToKafka("save.partition-0", kafkaConnect, "partition-0") ;
    builder.setBolt("save.partition-0", saveP0, 5).shuffleGrouping("split", "partition-0");
    
    SaveStreamToKafka saveP1 = new SaveStreamToKafka("save.partition-1", kafkaConnect, "partition-1") ;
    builder.setBolt("save.partition-1", saveP1, 5).shuffleGrouping("split", "partition-1");

    SaveStreamToKafka saveAll = new SaveStreamToKafka("save.all", kafkaConnect, outTopic) ;
    builder.setBolt("save.all", saveAll, 5).shuffleGrouping("split", "all");

    return builder;
  }
  
  public void submit(LocalCluster cluster) throws Exception {
    TopologyBuilder builder = createTopologyBuilder();
    Config conf = new Config();
    conf.setDebug(true);
    StormTopology topology = builder.createTopology();
    cluster.submitTopology("perftest", conf, topology);
  }
  
  static public void main(String[] args) throws Exception {
  }
}
