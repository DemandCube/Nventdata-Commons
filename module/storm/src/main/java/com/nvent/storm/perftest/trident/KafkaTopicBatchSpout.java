package com.nvent.storm.perftest.trident;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.nvent.tool.message.Message;
import com.nvent.util.JSONSerializer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

public class KafkaTopicBatchSpout<T> implements IBatchSpout  {
  private String                            name;
  private String                            topic;
  private String                            zkConnect;
  private Class<T>                          type;
  private ConsumerConnector                 consumer;
  private List<KafkaStream<byte[], byte[]>> streams;


  public KafkaTopicBatchSpout() {
  }

  public KafkaTopicBatchSpout(String name, String zkConnect, String topic, Class<T> type) {
    this.name         = name;
    this.zkConnect = zkConnect;
    this.topic        = topic;
    this.type         = type;
  }

  
  @Override
  public void open(Map conf, TopologyContext context) {
    Properties connectorProps = new Properties();
    connectorProps.put("group.id", name);
    connectorProps.put("zookeeper.connect", zkConnect);
    connectorProps.put("zookeeper.session.timeout.ms", "3000");
    connectorProps.put("zookeeper.sync.time.ms", "200");
    connectorProps.put("auto.commit.interval.ms", "1000");
    connectorProps.put("auto.commit.enable", "true");
    connectorProps.put("auto.offset.reset", "smallest");
    ConsumerConfig config = new ConsumerConfig(connectorProps);
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    streams = consumerMap.get(topic);
  }

  @Override
  public void emitBatch(long batchId, TridentCollector collector) {
    try {
      int allCount = 0;
      for(int i = 0; i < streams.size(); i++) {
        KafkaStream<byte[], byte[]> stream = streams.get(i) ;
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        int count = 0 ;
        while(count < 1000 && it.hasNext()) {
          MessageAndMetadata<byte[], byte[]> message = it.next() ;
          Message mObj = JSONSerializer.INSTANCE.fromBytes(message.message(), Message.class);
          mObj.setStartDeliveryTime(System.currentTimeMillis());
          collector.emit(new Values((Object) JSONSerializer.INSTANCE.toBytes(mObj)));
          count++;
          allCount++ ;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void ack(long batchId) {
  }

  @Override
  public void close() {
  }

  @Override
  public Map getComponentConfiguration() {
    return null;
  }

  @Override
  public Fields getOutputFields() { return new Fields("message"); }
}
