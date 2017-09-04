package com.github.uryyyyyyy.dataflow.simple;

import java.io.Serializable;
import java.util.List;

public class ListMyFileResult implements Serializable {

  final List<MyFileResult> results;

  ListMyFileResult(List<MyFileResult> results){
    this.results = results;
  }
}
