package com.nvent.flink.wordcount;

/**
 * This is the POJO (Plain Old Java Object) that is being used for all the operations.
 * As long as all fields are public or have a getter/setter, the system can handle them
 */
public class Word {
  String word;
  Integer frequency;

  public Word() { }
  
  public Word(String word, int i) {
    this.word = word;
    this.frequency = i;
  }
  
  // getters setters
  public String getWord() { return word; }
  public void setWord(String word) { this.word = word; }
  
  public Integer getFrequency() { return frequency; }
  public void setFrequency(Integer frequency) { this.frequency = frequency; }

  // to String
  @Override
  public String toString() { return "Word="+word+" freq="+frequency; }
}