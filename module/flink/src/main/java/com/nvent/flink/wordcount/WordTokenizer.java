package com.nvent.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class WordTokenizer implements FlatMapFunction<String, Word> {
  @Override
  public void flatMap(String value, Collector<Word> out) throws Exception {
    //normalize and split the line
    String[] tokens = value.toLowerCase().split("\\W+");
    //emit the pairs
    for (String token : tokens) {
      if (token.length() > 0) {
        out.collect(new Word(token, 1));
      }
    }
  }
} 
