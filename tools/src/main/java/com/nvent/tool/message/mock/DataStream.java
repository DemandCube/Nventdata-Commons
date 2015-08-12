package com.nvent.tool.message.mock;

import java.util.concurrent.LinkedBlockingQueue;

public class DataStream {
  private int                          streamId;
  private double                       duplicatedRatio = 0.01;
  private double                       lostRatio       = 0.01;
  private LinkedBlockingQueue<Message> queue;
  
  public DataStream(int streamId, int bufferSize, double duplicatedRatio, double lostRatio) {
    this.streamId = streamId;
    queue = new LinkedBlockingQueue<>(bufferSize);
    this.duplicatedRatio = duplicatedRatio;
    this.lostRatio = lostRatio;
  }
  
  public int getStreamId() { return this.streamId; }
  
  public DataStreamReader getReader() {
    return new DataStreamReader(queue) ;
  }
  
  public DataStreamWriter getWriter() {
    return new DataStreamWriter(queue, duplicatedRatio, lostRatio) ;
  }
}
