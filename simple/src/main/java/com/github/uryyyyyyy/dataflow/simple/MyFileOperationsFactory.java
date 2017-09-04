package com.github.uryyyyyyy.dataflow.simple;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.FileIOChannelFactory;
import com.google.cloud.dataflow.sdk.util.GcsIOChannelFactory;
import com.google.cloud.dataflow.sdk.util.IOChannelFactory;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;

import java.io.IOException;

class MyFileOperationsFactory {
  /**
   * Return a FileOperations implementation based on which IOChannel would be used to write to a
   * location specification (not necessarily a filename, as it may contain wildcards).
   *
   * <p>Only supports File and GCS locations (currently, the only factories registered with
   * IOChannelUtils). For other locations, an exception is thrown.
   */
  public static MyFileOperations getFileOperations(String spec, PipelineOptions options)
    throws IOException {
    IOChannelFactory factory = IOChannelUtils.getFactory(spec);
    if (factory instanceof GcsIOChannelFactory) {
      return new MyGcsOperations(options);
    } else if (factory instanceof FileIOChannelFactory) {
      return new MyLocalFileOperations();
    } else {
      throw new IOException("Unrecognized file system.");
    }
  }
}