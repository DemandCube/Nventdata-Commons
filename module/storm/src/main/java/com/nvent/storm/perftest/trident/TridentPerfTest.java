package com.nvent.storm.perftest.trident;

import com.nvent.kafka.perftest.KafkaMessageGenerator;
import com.nvent.kafka.perftest.KafkaMessageValidator;
import com.nvent.storm.perftest.KafkaTopicSpout;
import com.nvent.storm.perftest.PerfTestConfig;
import com.nvent.storm.perftest.SaveStreamToKafka;
import com.nvent.storm.perftest.SplitStream;
import com.nvent.tool.message.Message;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class TridentPerfTest {
  private PerfTestConfig config ;
  
  public TridentPerfTest(PerfTestConfig config ) {
    this.config = config ;
  }
  
  private TridentTopology createTridentTopology() {
    TridentTopology topology = new TridentTopology();
    KafkaTopicBatchSpout<Message> messageSpout = 
       new KafkaTopicBatchSpout<Message>("PerfTestSpout", config.zkConnect, config.topicIn, Message.class);
    Stream messageStream = topology.newStream("spout", messageSpout);
    messageStream.parallelismHint(3);
    for(int i = 0; i < config.numOPartition; i++) {
      Stream pStream = messageStream.each(new Fields("message"), new PartitionSelectorFunction("partition-" + i), new Fields());
      pStream.parallelismHint(3);
      SaveToKafkaFunction saveStreamFunc = new SaveToKafkaFunction("save." + "partition-" + i, config.kafkaConnect, "partition-" + i) ;
      pStream.each(new Fields("message"), saveStreamFunc, new Fields());
    }
    
    Stream allStream = messageStream.each(new Fields("message"), new PartitionSelectorFunction(null), new Fields());
    allStream.parallelismHint(3);
    SaveToKafkaFunction saveAllFunc = new SaveToKafkaFunction("save.all", config.kafkaConnect, config.topicOut) ;
    allStream.each(new Fields("message"), saveAllFunc, new Fields());
    return topology;
  }

  
  public void submit(LocalCluster cluster) throws Exception {
    TridentTopology topology = createTridentTopology();
    Config conf = new Config();
    conf.setDebug(true);
    cluster.submitTopology(config.stormTopologyName, conf, topology.build());
  }
  
  public void submit() throws Exception {
    System.setProperty("storm.jar", config.stormJarFiles);
    Config conf = new Config();
    conf.setDebug(true);
    conf.put(Config.NIMBUS_HOST, config.stormNimbusHost);
    conf.setNumWorkers(2 * 3);
    conf.setMaxSpoutPending(30000);    
    TridentTopology topology = createTridentTopology();
    StormSubmitter.submitTopologyWithProgressBar(config.stormTopologyName, conf, topology.build());
  }
  
  static public void main(String[] args) throws Exception {
    PerfTestConfig config = new PerfTestConfig(args);
    long start = System.currentTimeMillis() ;
    System.out.println(TridentPerfTest.class.getName() + "Start Generating The Messages");
    KafkaMessageGenerator messageGenerator = 
      new KafkaMessageGenerator(config.kafkaConnect, config.topicIn, config.numOPartition, config.numOfMessagePerPartition);
    messageGenerator.run();
    messageGenerator.waitForTermination(36000000);
    long generatorExecTime = System.currentTimeMillis() - start ;
    System.out.println("Message Generator Run In: " + generatorExecTime + "ms") ;
    
    start = System.currentTimeMillis() ;
    System.out.println(TridentPerfTest.class.getName() + "Start Submit And Launch PerfTest On The Storm Cluster");
    LocalCluster localCluster = null;
    
    TridentPerfTest perfTest = new TridentPerfTest(config);
    if(config.stormNimbusHost == null) {
      localCluster = new LocalCluster("localhost", new Long(2181));
      perfTest.submit(localCluster);
    } else {
      perfTest.submit() ;
    }
    
    
    if(config.stormNimbusHost != null) Thread.sleep(60000);

    System.out.println("Perf Test Generator Report:") ;
    System.out.println(messageGenerator.getTrackerReport()) ;
    
    System.out.println(TridentPerfTest.class.getName() + "Start Launching The Message Validator");
    KafkaMessageValidator validator =
      new KafkaMessageValidator(config.zkConnect, config.topicOut, config.numOPartition, config.numOfMessagePerPartition);
    validator.setConsumerTimeout(config.validatorConsumerTimeout);
    validator.run();
    validator.waitForTermination(3 * 3600000);
    
    System.out.println("Perf Test Generator Report:") ;
    System.out.println(messageGenerator.getTrackerReport()) ;
    
    System.out.println("Perf Test Validator Report:") ;
    System.out.println(validator.getTrackerReport()) ;
    
    long execTime = System.currentTimeMillis() - start ;
    System.out.println("Message Generator Run In: " + generatorExecTime + "ms") ;
    System.out.println("PerfTest And Validator Run In: " + execTime  + "ms") ;
    
    if(localCluster != null) localCluster.shutdown();
  }
}
