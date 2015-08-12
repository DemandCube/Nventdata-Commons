package com.nvent.tool.message.mock;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataStreamWriter {
  private double                       duplicatedRatio = 0.01;
  private double                       lostRatio       = 0.01;
  private Random                       duplicatedRandom = new Random();
  private Random                       lostRandom = new Random();
  private LinkedBlockingQueue<Message> queue;

  public DataStreamWriter(LinkedBlockingQueue<Message> queue, double duplicatedRatio, double lostRatio) {
    this.queue = queue;
    this.duplicatedRatio = duplicatedRatio;
    this.lostRatio = lostRatio;
  }

  public void write(Message message, long timeout) throws InterruptedException {
    if(lostRandom.nextDouble() <= lostRatio) return;
    queue.offer(message, timeout, TimeUnit.MILLISECONDS);
    if(duplicatedRandom.nextDouble() <= duplicatedRatio) {
      queue.offer(message, timeout, TimeUnit.MILLISECONDS);
    }
  }
}
