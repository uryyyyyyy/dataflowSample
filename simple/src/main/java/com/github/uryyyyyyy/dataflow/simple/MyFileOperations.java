package com.github.uryyyyyyy.dataflow.simple;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

interface MyFileOperations {
  /**
   * Copy a collection of files from one location to another.
   *
   * <p>The number of source filenames must equal the number of destination filenames.
   *
   * @param srcFilenames the source filenames.
   * @param destFilenames the destination filenames.
   */
  void copy(List<String> srcFilenames, List<String> destFilenames) throws IOException;

  /**
   * Remove a collection of files.
   */
  void remove(Collection<String> filenames) throws IOException;
}