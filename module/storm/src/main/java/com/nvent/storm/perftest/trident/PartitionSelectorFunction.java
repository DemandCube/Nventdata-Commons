package com.nvent.storm.perftest.trident;

import com.nvent.tool.message.Message;
import com.nvent.util.JSONSerializer;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class PartitionSelectorFunction extends BaseFunction {
  private String partition;
  
  public PartitionSelectorFunction(String partition) {
    this.partition = partition;
  }
  
  public void execute(TridentTuple tuple, TridentCollector collector) {
    byte[] data = tuple.getBinary(0);
    if(partition == null) {
      collector.emit(new Values(data));
      return ;
    }
    Message message = JSONSerializer.INSTANCE.fromBytes(data, Message.class);
    if(partition.equals(message.getPartition())) {
      collector.emit(new Values(data));
    }
  }
}
