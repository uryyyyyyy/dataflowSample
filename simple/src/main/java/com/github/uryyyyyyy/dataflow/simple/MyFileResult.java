package com.github.uryyyyyyy.dataflow.simple;

import java.io.Serializable;

public class MyFileResult implements Serializable {
  public final String uid;
  public final String filename;
  public final String dataKey;

  public MyFileResult(String uid, String filename, String dataKey) {
    if (dataKey == null) throw new RuntimeException("dataKey null");
    this.uid = uid;
    this.filename = filename;
    this.dataKey = dataKey;
  }
}