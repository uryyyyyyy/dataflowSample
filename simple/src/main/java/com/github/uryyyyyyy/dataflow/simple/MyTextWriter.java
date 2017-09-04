package com.github.uryyyyyyy.dataflow.simple;


import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.FileBasedSink;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.MimeTypes;
import com.google.cloud.dataflow.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

class MyTextWriter extends Sink.Writer<KV<String, Iterable<String>>, ListMyFileResult> {

  private static final Logger LOG = LoggerFactory.getLogger(FileBasedSink.FileBasedWriter.class);

  final MyTextWriteOperation writeOperation;

  /**
   * Unique id for this output bundle.
   */
  private String id;

  /**
   * The filename of the output bundle. Equal to the
   * {@link FileBasedSink.FileBasedWriteOperation#TEMPORARY_FILENAME_SEPARATOR} and id appended to
   * the baseName.
   */
  private String filename;

  private List<MyFileResult> results = new ArrayList<>();

  /**
   * The MIME type used in the creation of the output channel (if the file system supports it).
   *
   * <p>GCS, for example, supports writing files with Content-Type metadata.
   *
   * <p>May be overridden. Default is {@link MimeTypes#TEXT}. See {@link MimeTypes} for other
   * options.
   */
  protected String mimeType = MimeTypes.TEXT;

  /**
   * Opens the channel.
   */
  @Override
  public final void open(String uId) throws Exception {
    this.id = uId;
  }

  /**
   * Closes the channel and return the bundle result.
   */
  @Override
  public final ListMyFileResult close() throws Exception {
    return new ListMyFileResult(results);
  }

  /**
   * Return the FileBasedWriteOperation that this Writer belongs to.
   */
  @Override
  public MyTextWriteOperation getWriteOperation() {
    return writeOperation;
  }

  private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
  private final Coder<String> coder;
  @Nullable
  private final String header;
  @Nullable private final String footer;
  private OutputStream out;

  MyTextWriter(MyTextWriteOperation writeOperation, Coder<String> coder,
                    @Nullable String header, @Nullable String footer) {
    checkNotNull(writeOperation);
    this.writeOperation = writeOperation;
    this.header = header;
    this.footer = footer;
    this.mimeType = MimeTypes.TEXT;
    this.coder = coder;
  }

  /**
   * Writes {@code value} followed by a newline if {@code value} is not null.
   */
  private void writeIfNotNull(@Nullable String value) throws IOException {
    if (value != null) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
      out.write(NEWLINE);
    }
  }

  protected void prepareWrite(WritableByteChannel channel) throws Exception {
    out = Channels.newOutputStream(channel);
  }

  protected void writeHeader() throws Exception {
    writeIfNotNull(header);
  }

  protected void writeFooter() throws Exception {
    writeIfNotNull(footer);
  }

  @Override
  public void write(KV<String, Iterable<String>> value) throws Exception {

    filename = getWriteOperation().baseOutputFilename.get() + "temp-" + value.getKey();

    LOG.debug("Opening {}.", filename);
    WritableByteChannel channel = IOChannelUtils.create(filename, mimeType);
    try {
      prepareWrite(channel);
      LOG.debug("Writing header to {}.", filename);
      writeHeader();
    } catch (Exception e) {
      // The caller shouldn't have to close() this Writer if it fails to open(), so close the
      // channel if prepareWrite() or writeHeader() fails.
      try {
        LOG.error("Writing header to {} failed, closing channel.", filename);
        channel.close();
      } catch (IOException closeException) {
        // Log exception and mask it.
        LOG.error("Closing channel for {} failed: {}", filename, closeException.getMessage());
      }
      // Throw the exception that caused the write to fail.
      throw e;
    }
    LOG.debug("Starting write of bundle {} to {}.", this.id, filename);

    for(String vaaa : value.getValue()){
      coder.encode(vaaa, out, Coder.Context.OUTER);
      out.write(NEWLINE);
    }

    try (WritableByteChannel theChannel = channel) {
      LOG.debug("Writing footer to {}.", filename);
      writeFooter();
    }
    MyFileResult result = new MyFileResult(id, filename, value.getKey());
    LOG.debug("Result for bundle {}: {}", this.id, filename);
    results.add(result);
  }
}