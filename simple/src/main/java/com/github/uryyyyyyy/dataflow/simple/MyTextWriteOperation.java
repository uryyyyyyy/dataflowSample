package com.github.uryyyyyyy.dataflow.simple;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.FileBasedSink;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.ValueProvider;
import com.google.cloud.dataflow.sdk.util.IOChannelFactory;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

class MyTextWriteOperation extends Sink.WriteOperation<KV<String, Iterable<String>>, ListMyFileResult> {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedSink.FileBasedWriteOperation.class);

  /**
   * Options for handling of temporary output files.
   */
  public enum TemporaryFileRetention {
    KEEP,
    REMOVE;
  }

  /**
   * The Sink that this WriteOperation will write to.
   */
  final MyTextSink sink;

  /**
   * Option to keep or remove temporary output files.
   */
  final TemporaryFileRetention temporaryFileRetention;

  /**
   * Base filename used for temporary output files. Default is the baseOutputFilename.
   */
  final ValueProvider<String> baseOutputFilename;

  /**
   * Name separator for temporary files. Temporary files will be named
   * {@code {baseTemporaryFilename}-temp-{bundleId}}.
   */
  static final String TEMPORARY_FILENAME_SEPARATOR = "-temp-";

  /**
   * Build a temporary filename using the temporary filename separator with the given prefix and
   * suffix.
   */
  static final String buildTemporaryFilename(String prefix, String suffix) {
    return prefix + TEMPORARY_FILENAME_SEPARATOR + suffix;
  }

  /**
   * Initialization of the sink. Default implementation is a no-op. May be overridden by subclass
   * implementations to perform initialization of the sink at pipeline runtime. This method must
   * be idempotent and is subject to the same implementation restrictions as
   * {@link Sink.WriteOperation#initialize}.
   */
  @Override
  public void initialize(PipelineOptions options) throws Exception {}

  /**
   * Finalizes writing by copying temporary output files to their final location and optionally
   * removing temporary files.
   *
   * <p>Finalization may be overridden by subclass implementations to perform customized
   * finalization (e.g., initiating some operation on output bundles, merging them, etc.).
   * {@code writerResults} contains the filenames of written bundles.
   *
   * <p>If subclasses override this method, they must guarantee that its implementation is
   * idempotent, as it may be executed multiple times in the case of failure or for redundancy. It
   * is a best practice to attempt to try to make this method atomic.
   *
   * @param writerResults the results of writes (FileResult).
   */
  @Override
  public void finalize(Iterable<ListMyFileResult> writerResults, PipelineOptions options)
    throws Exception {
    // Collect names of temporary files and rename them.
    List<MyFileResult> files = new ArrayList<>();
    for (ListMyFileResult results : writerResults) {
      for (MyFileResult result : results.results) {
        LOG.debug("Temporary bundle output file {} will be copied.", result.filename);
        files.add(result);
      }
    }
    copyToOutputFiles(files, options);
  }

  /**
   * Copy temporary files to final output filenames using the file naming template.
   *
   * <p>Can be called from subclasses that override {@link FileBasedSink.FileBasedWriteOperation#finalize}.
   *
   * <p>Files will be named according to the file naming template. The order of the output files
   * will be the same as the sorted order of the input filenames.  In other words, if the input
   * filenames are ["C", "A", "B"], baseOutputFilename is "file", the extension is ".txt", and
   * the fileNamingTemplate is "-SSS-of-NNN", the contents of A will be copied to
   * file-000-of-003.txt, the contents of B will be copied to file-001-of-003.txt, etc.
   *
   * @param filenames the filenames of temporary files.
   * @return a list containing the names of final output files.
   */
  protected final List<String> copyToOutputFiles(List<MyFileResult> filenames, PipelineOptions options)
    throws IOException {
    int numFiles = filenames.size();
    String suffix = getFileExtension(getSink().extension);

    List<String> srcFilenames = filenames.stream().map(result -> result.filename).collect(Collectors.toList());
    List<String> destFilenames = filenames.stream().map(result -> baseOutputFilename.get() + result.dataKey + result.uid + suffix).collect(Collectors.toList());
//    List<String> destFilenames = filenames.stream().map(result -> baseOutputFilename + result.dataKey + "/" + result.uid + "-" + numFiles + suffix).collect(Collectors.toList());
      generateDestinationFilenames(numFiles);

    if (numFiles > 0) {
      LOG.debug("Copying {} files.", numFiles);
      MyFileOperations fileOperations =
        MyFileOperationsFactory.getFileOperations(destFilenames.get(0), options);
      fileOperations.copy(srcFilenames, destFilenames);
    } else {
      LOG.info("No output files to write.");
    }

    return destFilenames;
  }

  /**
   * Returns the file extension to be used. If the user did not request a file
   * extension then this method returns the empty string. Otherwise this method
   * adds a {@code "."} to the beginning of the users extension if one is not present.
   */
  static String getFileExtension(String usersExtension) {
    if (usersExtension == null || usersExtension.isEmpty()) {
      return "";
    }
    if (usersExtension.startsWith(".")) {
      return usersExtension;
    }
    return "." + usersExtension;
  }

  /**
   * Generate output bundle filenames.
   */
  protected final List<String> generateDestinationFilenames(int numFiles) {
    List<String> destFilenames = new ArrayList<>();
    String extension = getSink().extension;
    String baseOutputFilename = getSink().baseOutputFilename.get();
    String fileNamingTemplate = getSink().fileNamingTemplate;

    String suffix = getFileExtension(extension);
    for (int i = 0; i < numFiles; i++) {
      destFilenames.add(IOChannelUtils.constructName(
        baseOutputFilename, fileNamingTemplate, suffix, i, numFiles));
    }
    return destFilenames;
  }

  /**
   * Use {@link #removeTemporaryFiles(Collection, PipelineOptions)} instead.
   */
  @Deprecated
  protected final void removeTemporaryFiles(PipelineOptions options) throws IOException {
    removeTemporaryFiles(Collections.<String>emptyList(), options);
  }

  /**
   * Removes temporary output files. Uses the temporary filename to find files to remove.
   *
   * <p>Additionally, to partially mitigate the effects of filesystems with eventually-consistent
   * directory matching APIs, takes a list of files that are known to exist - i.e. removes the
   * union of the known files and files that the filesystem says exist in the directory.
   *
   * <p>Assumes that, if globbing had been strongly consistent, it would have matched all
   * of knownFiles - i.e. on a strongly consistent filesystem, knownFiles can be ignored.
   *
   * <p>Can be called from subclasses that override {@link FileBasedSink.FileBasedWriteOperation#finalize}.
   * <b>Note:</b>If finalize is overridden and does <b>not</b> rename or otherwise finalize
   * temporary files, this method will remove them.
   */
  protected final void removeTemporaryFiles(
    Collection<String> knownFiles, PipelineOptions options) throws IOException {
    String pattern = buildTemporaryFilename(baseOutputFilename.get(), "*");
    LOG.debug("Finding temporary bundle output files matching {}.", pattern);
    MyFileOperations fileOperations = MyFileOperationsFactory.getFileOperations(pattern, options);
    IOChannelFactory factory = IOChannelUtils.getFactory(pattern);
    Collection<String> matches = factory.match(pattern);
    Set<String> allMatches = new HashSet<>(matches);
    allMatches.addAll(knownFiles);
    LOG.debug(
      "Removing {} temporary files matching {} ({} matched glob, {} additional known files)",
      allMatches.size(),
      pattern,
      matches.size(),
      allMatches.size() - matches.size());
    fileOperations.remove(allMatches);
  }

  /**
   * Provides a coder for {@link FileBasedSink.FileResult}.
   */
  @Override
  public Coder<ListMyFileResult> getWriterResultCoder() {
    return SerializableCoder.of(ListMyFileResult.class);
  }

  /**
   * Returns the FileBasedSink for this write operation.
   */
  @Override
  public MyTextSink getSink() {
    return sink;
  }


  private final Coder<String> coder;
  @Nullable
  private final String header;
  @Nullable private final String footer;

  MyTextWriteOperation(MyTextSink sink, Coder<String> coder,
                             @Nullable String header, @Nullable String footer) {
    this.sink = sink;
    this.baseOutputFilename = sink.getBaseOutputFilenameProvider();
    this.temporaryFileRetention = TemporaryFileRetention.REMOVE;
    this.coder = coder;
    this.header = header;
    this.footer = footer;
  }

  public MyTextWriter createWriter(PipelineOptions options) throws Exception {
    return new MyTextWriter(this, coder, header, footer);
  }
}