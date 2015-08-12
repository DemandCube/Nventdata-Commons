package com.nvent.tool.message.mock;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataStreamReader {
  private LinkedBlockingQueue<Message> queue;

  public DataStreamReader(LinkedBlockingQueue<Message> queue) {
    this.queue = queue ;
  }
  
  public Message next(long timeout) throws InterruptedException {
    return queue.poll(timeout, TimeUnit.MILLISECONDS);
  }
}
