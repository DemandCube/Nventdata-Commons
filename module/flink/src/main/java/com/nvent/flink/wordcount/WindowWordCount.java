package com.nvent.flink.wordcount;

import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.helper.Time;

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 * 
 * <p>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.examples.java.wordcount.util.WordCountData}.
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>use basic windowing abstractions.
 * </ul>
 *
 */
public class WindowWordCount {

  private static String textPath    = "build.gradle";
  private static String outputPath  = "build/wordcount/window";
  
  // *************************************************************************
  // PROGRAM
  // *************************************************************************

  public static void main(String[] args) throws Exception {
    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // get input data
    DataStream<String> text = env.readTextFile(textPath);

    DataStream<Word> counts =
        text.flatMap(new WordTokenizer()). // split up the lines in pairs (2-tuples) containing: (word,1)
        window(Time.of(5, TimeUnit.MILLISECONDS)). //trigger base on the time window
        every(Count.of(10)).   //trigger base on the number of elements
        groupBy("word").reduceWindow(new WordReducer()). // group by the tuple field "0" and sum up tuple field "1"
        flatten(); // flatten the windows to a single stream

    counts.writeAsText(outputPath);
    counts.print();
    // execute program
    env.execute("Window WordCount");
  }
}
