package com.nvent.tool.message;

import java.util.BitSet;
import java.util.Map;
import java.util.TreeMap;

import com.nvent.util.text.TabularFormater;

public class BitSetMessageTracker {
  private int expectNumOfMessagePerPartition ;
  private TreeMap<String, BitSetPartitionMessageTracker> partitions = new TreeMap<>();
  
  public BitSetMessageTracker(int expectNumOfMessage) {
    this.expectNumOfMessagePerPartition = expectNumOfMessage;
  }
  
  synchronized public void log(String partition, int index) {
    BitSetPartitionMessageTracker pTracker = partitions.get(partition) ;
    if(pTracker == null) {
      pTracker = new BitSetPartitionMessageTracker(expectNumOfMessagePerPartition);
      partitions.put(partition, pTracker);
    }
    pTracker.log(index);;
  }
 
  public String[] getPartitions() {
    String[] names = new String[partitions.size()];
    partitions.keySet().toArray(names);
    return names;
  }
  
  public BitSetPartitionMessageTracker getPartitionTracker(String name) { return partitions.get(name); }
  
  public String getFormatedReport() {
    TabularFormater formater = new TabularFormater("Partition", "Expect", "Lost", "Duplicated");
    for(Map.Entry<String, BitSetPartitionMessageTracker> entry : partitions.entrySet()) {
      BitSetPartitionMessageTracker tracker = entry.getValue();
      formater.addRow(entry.getKey(), tracker.getExpect(), tracker.getLostCount(), tracker.getDuplicatedCount());
    }
    return formater.getFormattedText();
  }
  
  static public class BitSetPartitionMessageTracker {
    private BitSet bitSet ;
    private int duplicatedCount = 0;
    private int numOfBits ;
    
    public BitSetPartitionMessageTracker(int nBits) {
      this.numOfBits = nBits ;
      bitSet = new BitSet(nBits) ;
    }
    
    public void log(int idx) {
      if(idx > numOfBits) {
        throw new RuntimeException("the index is bigger than expect num of bits " + numOfBits);
      }
      if(bitSet.get(idx)) duplicatedCount++ ;
      bitSet.set(idx, true);
    }
    
    public int getExpect() { return numOfBits; }
    
    public int getDuplicatedCount() { return this.duplicatedCount ; }
   
    public int getLostCount() {
      int lostCount = 0;
      for(int i = 0; i < numOfBits; i++) {
        if(!bitSet.get(i)) lostCount++ ;
      }
      return lostCount;
    }
  }
}