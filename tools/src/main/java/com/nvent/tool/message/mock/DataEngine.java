package com.nvent.tool.message.mock;

import java.util.TreeMap;

public class DataEngine {
  private TreeMap<Integer, DataStream> streams ;
  
  public DataEngine(int numOfStream, int streamBufferSize, double duplicatedRatio, double lostRatio) {
    streams = new TreeMap<>();
    for(int i = 0; i < numOfStream; i++) {
      streams.put(i, new DataStream(i, streamBufferSize, duplicatedRatio, lostRatio));
    }
  }
  
  public DataStream[] getDataStream() {
    DataStream[] stream = new DataStream[streams.size()] ;
    for(int i = 0; i < stream.length; i++) {
      stream[i] = streams.get(i);
    }
    return stream;
  }
}
