package com.github.uryyyyyyy.dataflow.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class MyLocalFileOperations implements MyFileOperations {
  private static final Logger LOG = LoggerFactory.getLogger(MyLocalFileOperations.class);

  @Override
  public void copy(List<String> srcFilenames, List<String> destFilenames) throws IOException {
    checkArgument(
      srcFilenames.size() == destFilenames.size(),
      "Number of source files %s must equal number of destination files %s",
      srcFilenames.size(),
      destFilenames.size());
    int numFiles = srcFilenames.size();
    for (int i = 0; i < numFiles; i++) {
      String src = srcFilenames.get(i);
      String dst = destFilenames.get(i);
      LOG.debug("Copying {} to {}", src, dst);
      copyOne(src, dst);
    }
  }

  private void copyOne(String source, String destination) throws IOException {
    try {
      // Copy the source file, replacing the existing destination.
      Files.copy(Paths.get(source), Paths.get(destination), StandardCopyOption.REPLACE_EXISTING);
    } catch (NoSuchFileException e) {
      LOG.debug("{} does not exist.", source);
      // Suppress exception if file does not exist.
    }
  }

  @Override
  public void remove(Collection<String> filenames) throws IOException {
    for (String filename : filenames) {
      LOG.debug("Removing file {}", filename);
      removeOne(filename);
    }
  }

  private void removeOne(String filename) throws IOException {
    // Delete the file if it exists.
    boolean exists = Files.deleteIfExists(Paths.get(filename));
    if (!exists) {
      LOG.debug("{} does not exist.", filename);
    }
  }
}