package com.nvent.storm.perftest;

import java.util.Map;

import com.nvent.kafka.producer.DefaultKafkaWriter;
import com.nvent.tool.message.Message;
import com.nvent.util.JSONSerializer;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SaveStreamToKafka extends BaseBasicBolt {
  private String name ;
  private String kafkaConnect ;
  private String topic ;
  transient private DefaultKafkaWriter writer;
  
  public SaveStreamToKafka() {}
      
  public SaveStreamToKafka(String name, String kafkaConnect, String topic) {
    this.name = name; 
    this.kafkaConnect = kafkaConnect;
    this.topic = topic ;
  }
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    System.out.println("SaveStream: prepare(...)") ;
    writer = new DefaultKafkaWriter(name, kafkaConnect);
  }

  @Override
  public void cleanup() {
    System.out.println("SaveStream: cleanup(...)") ;
    if(writer != null) {
      writer.close();
    }
  }
  
  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    try {
      byte[] data = input.getBinary(0);
      Message message = JSONSerializer.INSTANCE.fromBytes(data, Message.class);
      message.setEndDeliveryTime(System.currentTimeMillis());
      writer.send(topic, message, 5000);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}