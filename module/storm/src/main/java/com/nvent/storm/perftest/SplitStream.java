package com.nvent.storm.perftest;

import java.util.Map;

import com.nvent.tool.message.Message;
import com.nvent.util.JSONSerializer;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitStream extends BaseBasicBolt {
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
  }

  @Override
  public void cleanup() {
  }
  
  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    byte[] data = input.getBinary(0);
    Message message = JSONSerializer.INSTANCE.fromBytes(data, Message.class);
    String partition = message.getPartition();
    //partition = partition.replace("_", "");
    collector.emit(partition, new Values((Object)data));
    collector.emit("all", new Values(data));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message"));
    declarer.declareStream("partition-0", new Fields("partition"));
    declarer.declareStream("partition-1", new Fields("partition"));
    declarer.declareStream("all",         new Fields("partition"));
  }
}