package com.nvent.kafka.consumer;


import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.zookeeper.server.EmbededZKServer;
import com.nvent.kafka.producer.DefaultKafkaWriter;
import com.nvent.kafka.server.EmbededKafkaServer;
import com.nvent.util.io.FileUtil;

public class KafkaMessageConsumerConnectorUnitTest {
  private  EmbededZKServer    zkServer ;
  private  EmbededKafkaServer kafkaServer ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/server", false);
    zkServer = new EmbededZKServer("build/server/zookeeper", 2181);
    zkServer.start();
    kafkaServer = new EmbededKafkaServer(0, "build/server/kafka", 9092);
    kafkaServer.start();
  }
  
  @After
  public void teardown() throws Exception {
    kafkaServer.shutdown();
    zkServer.shutdown();
  }
  @Test
  public void testReader() throws Exception {
    String NAME = "test";
    String TOPIC = "hello";
    DefaultKafkaWriter writer = new DefaultKafkaWriter(NAME, kafkaServer.getConnectString());
    for(int i = 0; i < 100; i++) {
      String hello = "Hello " + i;
      writer.send(TOPIC, "key-" + i, hello, 5000);
    }
    writer.close();
    
    KafkaMessageConsumerConnector connector = 
        new KafkaMessageConsumerConnector(NAME, zkServer.getConnectString()).
        withConsumerTimeoutMs(1000).
        connect();
   
    MessageConsumerHandler handler = new MessageConsumerHandler() {
      @Override
      public void onMessage(String topic, byte[] key, byte[] message) {
        String mesg = new String(message);
        System.out.println("on message: " + mesg);
      }
    };
    connector.consume(TOPIC, handler, 1);
    connector.awaitTermination(5000, TimeUnit.MILLISECONDS);
  }
}
