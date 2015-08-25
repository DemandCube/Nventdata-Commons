package com.nvent.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreaming {
  static FlatMapFunction<String, Word> WORD_TOKENIZER = new WordTokenizer() ;
  static ReduceFunction<Word> WORD_REDUCER = new WordReducer(); 
  
  public static void main(String[] args) throws Exception {
    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
    // get input data
    DataStream<String> text = env.socketTextStream("localhost", 9999, '\n', 0);
    DataStream<Word> counts = text.flatMap(WORD_TOKENIZER).groupBy("word").reduce(WORD_REDUCER);
    
    counts.writeAsText("build/wordcount-streaming/output", WriteMode.OVERWRITE);
    counts.print();
    env.execute("WordCount Streaming Example");
  }
}
