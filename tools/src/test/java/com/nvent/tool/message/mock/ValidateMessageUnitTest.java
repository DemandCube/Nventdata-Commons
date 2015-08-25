package com.nvent.tool.message.mock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.nvent.tool.message.BitSetMessageTracker;
import com.nvent.tool.message.Message;

public class ValidateMessageUnitTest {
  @Test
  public void testValidate() throws InterruptedException {
    int NUM_OF_STREAM             = 5 ;
    int NUM_OF_MESSAGE_PER_STREAM = 1000 ;
    
    DataEngine engine = 
      new DataEngine(NUM_OF_STREAM, 1000, 0.01/*duplicated ratio*/, 0.00/*lost ratio*/);
    DataStream[] stream = engine.getDataStream();
    
    BitSetMessageTracker generateTracker = new BitSetMessageTracker(NUM_OF_MESSAGE_PER_STREAM );
    ExecutorService generatorService = Executors.newFixedThreadPool(stream.length);
    for(DataStream selStream : stream) {
      generatorService.submit(new MessageStreamGenerator(generateTracker, selStream, NUM_OF_MESSAGE_PER_STREAM));
    }
    generatorService.shutdown();
    
    BitSetMessageTracker validateTracker = new BitSetMessageTracker(NUM_OF_MESSAGE_PER_STREAM );
    ExecutorService validatorService = Executors.newFixedThreadPool(stream.length);
    for(DataStream selStream : stream) {
      validatorService.submit(new MessageStreamValidator(validateTracker, selStream));
    }
    validatorService.shutdown();
    
    generatorService.awaitTermination(5, TimeUnit.MINUTES);
    validatorService.awaitTermination(5, TimeUnit.MINUTES);
 
    System.out.println("Message Generator:");
    System.out.println(generateTracker.getFormatedReport());
    
    System.out.println("Message Validator:");
    System.out.println(validateTracker.getFormatedReport());
  }
  
  static public class MessageStreamGenerator implements Runnable {
    private BitSetMessageTracker tracker;
    private DataStream           dataStream;
    private int                  numOfMessage ;
    
    public MessageStreamGenerator(BitSetMessageTracker tracker, DataStream stream, int numOfMessage) {
      this.tracker      = tracker;
      this.dataStream   = stream;
      this.numOfMessage = numOfMessage;
    }
    
    @Override
    public void run() {
      DataStreamWriter writer = dataStream.getWriter() ;
      String partition = "stream " + dataStream.getStreamId();
      try {
        for(int i = 0; i < numOfMessage; i++) {
          Message message = new Message(partition, i, "key-" + i, new byte[128]) ;
          writer.write(message, 5000);
          tracker.log(partition, i);
        }
      } catch(InterruptedException ex) {
      }
    }
  }
  
  static public class MessageStreamValidator implements Runnable {
    private BitSetMessageTracker tracker;
    private DataStream           dataStream;
    
    public MessageStreamValidator(BitSetMessageTracker tracker, DataStream stream) {
      this.tracker      = tracker;
      this.dataStream   = stream;
    }
    
    @Override
    public void run() {
      DataStreamReader reader = dataStream.getReader() ;
      try {
        Message message = null ;
        while((message = reader.next(5000)) != null) {
          tracker.log(message.getPartition(), message.getTrackId());
        }
      } catch(InterruptedException ex) {
      }
    }
  }
}