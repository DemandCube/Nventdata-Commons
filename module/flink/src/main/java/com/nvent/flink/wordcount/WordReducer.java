package com.nvent.flink.wordcount;

import org.apache.flink.api.common.functions.ReduceFunction;

public class WordReducer implements ReduceFunction<Word> {
  @Override
  public Word reduce(Word value1, Word value2) throws Exception {
    return new Word(value1.word,value1.frequency + value2.frequency);
  }
}
