package com.github.uryyyyyyy.dataflow.simple;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.ValueProvider;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.values.KV;

import javax.annotation.Nullable;

class MyTextSink extends Sink<KV<String, Iterable<String>>> {
  /**
   * Base filename for final output files.
   */
  protected final ValueProvider<String> baseOutputFilename;

  /**
   * The extension to be used for the final output files.
   */
  protected final String extension;

  protected final String fileNamingTemplate;

  /**
   * Returns the base output filename for this file based sink.
   */
  public String getBaseOutputFilename() {
    return baseOutputFilename.get();
  }

  /**
   * Returns the base output filename for this file based sink.
   */
  public ValueProvider<String> getBaseOutputFilenameProvider() {
    return baseOutputFilename;
  }

  /**
   * Perform pipeline-construction-time validation. The default implementation is a no-op.
   * Subclasses should override to ensure the sink is valid and can be written to. It is recommended
   * to use {@link com.google.common.base.Preconditions} in the implementation of this method.
   */
  @Override
  public void validate(PipelineOptions options) {}

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    String fileNamePattern = String.format("%s%s%s",
      baseOutputFilename.isAccessible()
        ? baseOutputFilename.get() : baseOutputFilename.toString(),
      fileNamingTemplate, getFileExtension(extension));
    builder.add(DisplayData.item("fileNamePattern", fileNamePattern)
      .withLabel("File Name Pattern"));
  }

  /**
   * Returns the file extension to be used. If the user did not request a file
   * extension then this method returns the empty string. Otherwise this method
   * adds a {@code "."} to the beginning of the users extension if one is not present.
   */
  private static String getFileExtension(String usersExtension) {
    if (usersExtension == null || usersExtension.isEmpty()) {
      return "";
    }
    if (usersExtension.startsWith(".")) {
      return usersExtension;
    }
    return "." + usersExtension;
  }

  private final Coder<String> coder;
  @Nullable
  private final String header;
  @Nullable
  private final String footer;

  MyTextSink(
    ValueProvider<String> baseOutputFilename, String extension,
    @Nullable String header, @Nullable String footer,
    String fileNameTemplate, Coder<String> coder) {
    this.baseOutputFilename = baseOutputFilename;
    this.extension = extension;
    this.fileNamingTemplate = fileNameTemplate;
    this.coder = coder;
    this.header = header;
    this.footer = footer;
  }

  @Override
  public MyTextWriteOperation createWriteOperation(PipelineOptions options) {
    return new MyTextWriteOperation(this, coder, header, footer);
  }
}