package com.github.uryyyyyyy.dataflow.simple;


import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class Main2 {

  static class ExtractWordsFn extends DoFn<String, String> {
    private final Aggregator<Long, Long> emptyLines =
      createAggregator("emptyLines", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
      if (c.element().trim().isEmpty()) {
        emptyLines.addValue(1L);
      }

      // Split the line into words.
      String[] words = c.element().split("[^a-zA-Z']+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  /** A SimpleFunction that converts a Word and Count into a printable string. */
  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  public static class CountWords extends PTransform<PCollection<String>,
    PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
        ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
        words.apply(Count.perElement());

      return wordCounts;
    }
  }

  public interface WordCountOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
    String getInputFile();
    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Default.InstanceFactory(OutputFactory.class)
    String getOutput();
    void setOutput(String value);

    /**
     * Returns "gs://${YOUR_STAGING_DIRECTORY}/counts.txt" as the default destination.
     */
    class OutputFactory implements DefaultValueFactory<String> {
      @Override
      public String create(PipelineOptions options) {
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        if (dataflowOptions.getStagingLocation() != null) {
          return GcsPath.fromUri(dataflowOptions.getStagingLocation())
            .resolve("counts.txt").toString();
        } else {
          throw new IllegalArgumentException("Must specify --output or --stagingLocation");
        }
      }
    }

  }

  public static void main(String[] args) {
    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(WordCountOptions.class);
    Pipeline p = Pipeline.create(options);

    p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile()))
      .apply(new CountWords())
      .apply(MapElements.via(new FormatAsTextFn()))
      .apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));

    p.run();
  }
}