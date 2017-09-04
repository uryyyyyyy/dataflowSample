package com.github.uryyyyyyy.dataflow.simple;

import com.google.cloud.dataflow.sdk.io.FileBasedSink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.GcsUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class MyGcsOperations implements MyFileOperations {
  private final GcsUtil gcsUtil;

  public MyGcsOperations(PipelineOptions options) {
    gcsUtil = new GcsUtil.GcsUtilFactory().create(options);
  }

  @Override
  public void copy(List<String> srcFilenames, List<String> destFilenames) throws IOException {
    gcsUtil.copy(srcFilenames, destFilenames);
  }

  @Override
  public void remove(Collection<String> filenames) throws IOException {
    gcsUtil.remove(filenames);
  }
}