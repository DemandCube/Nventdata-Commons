package com.nvent.flink.perftest;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.helper.Time;

import com.nvent.kafka.perftest.KafkaMessageGenerator;
import com.nvent.kafka.perftest.KafkaMessageValidator;
import com.nvent.tool.message.Message;

public class PerfTest {
  
  private PerfTestConfig config;
  
  public PerfTest(PerfTestConfig config) {
    this.config = config;
  }
  
  public void run() throws Exception {
    //set up the execution environment
    StreamExecutionEnvironment env = null ;
    if(config.flinkJobManagerHost != null) {
      String[] jarFiles    = config.flinkJarFiles.split(",") ;
      String   host        = config.flinkJobManagerHost;
      int      port        = config.flinkJobManagerPort;
      int      parallelism = config.flinkParallelism;
      env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, parallelism, jarFiles);
    } else {
      env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(3);
    }
    
    KafkaMessageStreamFunction kafkaMessageStreamFunc = 
      new KafkaMessageStreamFunction("PerfTest", config.zkConnect, config.topicIn, Message.class) ;
    
    DataStream<Message> messageStream  = env.addSource(kafkaMessageStreamFunc);
    
    DataStream<Message> flattenStream = 
        messageStream.
        window(Time.of(10, TimeUnit.MILLISECONDS)). //trigger base on the time window
        every(Count.of(100)).   //trigger base on the number of elements
        flatten();
    
    MessageOutputSelector outSelector = new MessageOutputSelector() ;
    
    SplitDataStream<Message> split = flattenStream.split(outSelector);
    for(int i = 0; i < config.numOPartition; i++) {
      DataStream<Message> partition  = split.select("partition-" + i);
      partition.writeAsText(config.outputPath + "/partition-" + i, WriteMode.OVERWRITE);
    }
    
    DataStream<Message> all = split.select("all");
    
    KafkaSinkFunction<Message> kafkaAllSink = 
      new MessageKafkaSinkFunction("perftestOut", config.kafkaConnect, config.topicOut) ;
    all.addSink(kafkaAllSink);

    //execute program
    env.execute("Perf Test");
  }
  
  static public class KafkaMessageStreamFunction extends KafkaStreamFunction<Message> {
    private static final long serialVersionUID = 1L;

    public KafkaMessageStreamFunction() {} 

    public KafkaMessageStreamFunction(String name, String zkConnect, String topic, Class<Message> type) {
      super("PerfTest", zkConnect, topic, type);
    }

    public void beforeCollect(Message mObj) {
      mObj.setStartDeliveryTime(System.currentTimeMillis());
    }
  }
  
  static public class MessageOutputSelector implements OutputSelector<Message> {
    private static final long serialVersionUID = 1L;

    @Override
    public Iterable<String> select(Message value) {
      ArrayList<String> names = new ArrayList<>();
      names.add(value.getPartition());
      names.add("all");
      return names;
    }
  };
    
  static public class MessageKafkaSinkFunction extends KafkaSinkFunction<Message> {
    private static final long serialVersionUID = 1L;

    public MessageKafkaSinkFunction() {} 
    
    public MessageKafkaSinkFunction(String name, String kafkaConnect, String topic) {
      super(name, kafkaConnect, topic);
    }
    
    public void invoke(Message message) throws Exception {
      message.setEndDeliveryTime(System.currentTimeMillis());
      super.invoke(message);
    }
  }
  
  public static void main(String[] args) throws Exception {
    long start = System.currentTimeMillis() ;
    PerfTestConfig config = new PerfTestConfig(args) ;
    KafkaMessageGenerator messageGenerator = 
      new KafkaMessageGenerator(config.kafkaConnect, config.topicIn, config.numOPartition, config.numOfMessagePerPartition);
    messageGenerator.setMessageSize(config.messageSize);
    messageGenerator.run();
    
    PerfTest perfTest = new PerfTest(config);
    perfTest.run();
    System.out.println("Perf Test Generator Report:") ;
    System.out.println(messageGenerator.getTrackerReport()) ;
   
    KafkaMessageValidator validator =
      new KafkaMessageValidator(config.zkConnect, config.topicOut, 2, config.numOfMessagePerPartition);
    validator.run();
    validator.waitForTermination(60000);
    System.out.println("Perf Test Validator Report:") ;
    System.out.println(validator.getTrackerReport()) ;
    long execTime = System.currentTimeMillis() - start ;
    System.out.println("Execute Time: " + execTime + "ms");
  }
}
