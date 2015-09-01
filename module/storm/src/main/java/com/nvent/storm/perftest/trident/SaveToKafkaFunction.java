package com.nvent.storm.perftest.trident;

import java.util.Map;

import com.nvent.kafka.producer.DefaultKafkaWriter;
import com.nvent.tool.message.Message;
import com.nvent.util.JSONSerializer;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class SaveToKafkaFunction extends BaseFunction {
  private String name ;
  private String kafkaConnect ;
  private String topic ;
  transient private DefaultKafkaWriter writer;
  
  public SaveToKafkaFunction() {}
      
  public SaveToKafkaFunction(String name, String kafkaConnect, String topic) {
    this.name = name; 
    this.kafkaConnect = kafkaConnect;
    this.topic = topic ;
  }
  
  @Override
  public void prepare(Map conf, TridentOperationContext context) {
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
  
  public void execute(TridentTuple tuple, TridentCollector collector) {
    try {
      byte[] data = tuple.getBinary(0);
      Message message = JSONSerializer.INSTANCE.fromBytes(data, Message.class);
      message.setEndDeliveryTime(System.currentTimeMillis());
      writer.send(topic, message, 5000);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
