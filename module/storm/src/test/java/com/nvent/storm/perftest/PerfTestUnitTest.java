package com.nvent.storm.perftest;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.zookeeper.server.EmbededZKServer;
import com.nvent.kafka.server.EmbededKafkaServer;

import backtype.storm.LocalCluster;

public class PerfTestUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }
  
  private  EmbededZKServer    zkServer ;
  private  EmbededKafkaServer kafkaServer ;

  @Before
  public void setup() throws Exception {
    FileUtils.deleteDirectory(new File("build/perftest"));
    FileUtils.deleteDirectory(new File("build/server"));
    
    zkServer = new EmbededZKServer("build/server/zookeeper", 2181);
    zkServer.start();
    kafkaServer = new EmbededKafkaServer(0, "build/server/kafka", 9092);
    kafkaServer.setNumOfPartition(5);
    kafkaServer.start();
  }
  
  @After
  public void teardown() throws Exception {
    kafkaServer.shutdown();
    zkServer.shutdown();
  }
  
  @Test
  public void test() throws Exception {
    String[] args = {
      "--zk-connect", zkServer.getConnectString(),
      "--kafka-connect", kafkaServer.getConnectString(),
      "--num-of-partition", "2",
      "--num-of-message-per-partition", "10000",
      "--message-size", "512"
    };
    PerfTest.main(args);
  }
}
