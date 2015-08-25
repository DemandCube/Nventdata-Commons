package com.nvent.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;


@SuppressWarnings("serial")
public class WordCount {
  static FlatMapFunction<String, Word> WORD_TOKENIZER = new WordTokenizer() ;
  static ReduceFunction<Word> WORD_REDUCER = new WordReducer(); 
  
  private String  textPath;
  private String  outputPath;

  public WordCount(String[] args) {
    System.out.println("Executing WordCount example with built-in default data.");
    System.out.println("  Provide parameters to read input data from a file.");
    System.out.println("  Usage: WordCount <text path> <result path>");
    
    textPath = args[0];
    outputPath = args[1];
  }

  private LocalEnvironment createLocalEnvironment() {
    LocalEnvironment localEnv = ExecutionEnvironment.createLocalEnvironment();
    localEnv.setParallelism(3);
    return localEnv;
  }
  
  public void run() throws Exception {
    ExecutionEnvironment env = this.createLocalEnvironment();
    DataSet<String> text = env.readTextFile(textPath);
    
    DataSet<Word> counts = text.flatMap(WORD_TOKENIZER).groupBy("word").reduce(WORD_REDUCER);
    
    counts.writeAsText(outputPath, WriteMode.OVERWRITE);
    env.execute("WordCount Example");
    counts.print();
  }
  
  public static void main(String[] args) throws Exception {
    if(args == null || args.length == 0) {
      args = new String[] {"build.gradle", "build/wordcount.txt"} ;
    }
    WordCount wordCount = new WordCount(args);
    wordCount.run();
  }
}