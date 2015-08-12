package com.nvent.tool.message.mock;

public class Message {
  //partition or stream name or id
  private String  partition;
  private int     trackId ;
  private byte[]  key ;
  private byte[]  data;
  
  public Message() {}
  
  public Message(String  partition, int trackId, byte[] key, byte[] data) {
    this.partition = partition;
    this.trackId = trackId;
    this.key     = key ;
    this.data    = data ;
  }
  
  public Message(String partition, int trackId, String key, byte[] data) {
    this(partition, trackId, key.getBytes(), data);
  }

  public String getPartition() { return partition; }
  public void   setPartition(String partition) { this.partition = partition; }

  public int    getTrackId() { return trackId; }
  public void   setTrackId(int trackId) { this.trackId = trackId; }

  public byte[] getKey() { return key; }
  public void   setKey(byte[] key) { this.key = key; }

  public byte[] getData() { return data; }
  public void   setData(byte[] data) { this.data = data; }
}