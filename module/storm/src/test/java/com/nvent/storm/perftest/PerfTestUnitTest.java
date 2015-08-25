package com.nvent.storm.perftest;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.zookeeper.server.EmbededZKServer;
import com.nvent.kafka.perftest.KafkaMessageGenerator;
import com.nvent.kafka.perftest.KafkaMessageValidator;
import com.nvent.kafka.server.EmbededKafkaServer;

import backtype.storm.LocalCluster;

public class PerfTestUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }
  
  private  EmbededZKServer    zkServer ;
  private  EmbededKafkaServer kafkaServer ;
  private  LocalCluster cluster ;
  @Before
  public void setup() throws Exception {
    FileUtils.deleteDirectory(new File("build/perftest"));
    FileUtils.deleteDirectory(new File("build/server"));
    zkServer = new EmbededZKServer("build/server/zookeeper", 2181);
    zkServer.start();
    kafkaServer = new EmbededKafkaServer(0, "build/server/kafka", 9092);
    kafkaServer.setNumOfPartition(5);
    kafkaServer.start();
    cluster = new LocalCluster("localhost", new Long(2181));
  }
  
  @After
  public void teardown() throws Exception {
    cluster.shutdown();
    kafkaServer.shutdown();
    zkServer.shutdown();
  }
  
  @Test
  public void test() throws Exception {
    int NUM_OF_MSG_PER_PARTITION = 10000 ;
    String kafkaConnect = kafkaServer.getConnectString();
    String zkConnect = zkServer.getConnectString();
    KafkaMessageGenerator messageGenerator = 
      new KafkaMessageGenerator(kafkaConnect, PerfTest.IN_TOPIC, 2, NUM_OF_MSG_PER_PARTITION);
    messageGenerator.run();
    
    PerfTest perfTest = new PerfTest(new String[] {});

    perfTest.submit(cluster);
    Thread.sleep(45000);

    System.out.println("Perf Test Generator Report:") ;
    System.out.println(messageGenerator.getTrackerReport()) ;
    
    KafkaMessageValidator validator =
      new KafkaMessageValidator(zkConnect, PerfTest.OUT_TOPIC, 2, NUM_OF_MSG_PER_PARTITION);
    validator.run();
    validator.waitForTermination(60000);
    System.out.println("Perf Test Validator Report:") ;
    System.out.println(validator.getTrackerReport()) ;
  }
}
