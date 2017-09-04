package com.github.uryyyyyyy.dataflow.simple;


import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.options.ValueProvider.StaticValueProvider;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Main3 {

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

  public static class CountValues extends DoFn<String, KV<String, String>> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      String from = c.element();
      c.output(KV.of(String.valueOf(from.length()), from));
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

  public static class Hoge extends Combine.CombineFn<String, String[], MyFileResult> {

    @Override
    public List<String> createAccumulator() {
      return wrap(identity());
    }

    @Override
    public List<String> addInput(List<String> accumulator, String input) {
      accumulator.add(input);
      List<String> res = new ArrayList<String>();
      res.addAll(accumulator);
      return res;
    }

    @Override
    public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
      Iterator<List<String>> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        List<String> running = iter.next();
        while (iter.hasNext()) {
          running[0] = apply(running[0], iter.next()[0]);
        }
        return running;
      }
    }

    @Override
    public MyFileResult extractOutput(List<String> accumulator) {
      return null;
    }
  }

  public static class Hoge2 implements SerializableFunction<String, Integer> {

    @Override
    public Integer apply(String input) {
      return null;
    }
  }

  public static void main(String[] args) throws IOException {

    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(WordCountOptions.class);
    Pipeline p = Pipeline.create(options);

    PCollection<String> pStr = p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile()));
//    PCollection<KV<String, Long>> pKV = pStr.apply(new CountWords());
    PCollection<String> pStr2= pStr.apply(ParDo.of(new ExtractWordsFn()));
    PCollection<KV<String, String>> pKV = pStr2.apply(ParDo.of(new CountValues()));

    PCollection<KV<String, Iterable<String>>> aa = pKV.apply(Combine.<String, List<String>, MyFileResult>perKey(new Hoge()));

    PCollection<KV<String, Double>> salesRecords = null;
    PCollection<KV<String, Double>> totalSalesPerPerson =
      salesRecords.apply(Combine.<String, Double, Double>perKey(new Sum.SumDoubleFn()));


    PCollection<KV<String, Iterable<String>>> pKV33 = pKV.apply(Combine.<String, String, Iterable<String>>perKey(new Hoge()).withHotKeyFanout(new Hoge2()));
    PCollection<KV<String, Iterable<String>>> pKV2 = pKV.apply(GroupByKey.create());
//    PCollection<String> pStr2 = pKV.apply(MapElements.via(new FormatAsTextFn()));
//    PDone done = pStr2.apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));
    MyTextSink my = new MyTextSink(StaticValueProvider.of(options.getOutput()), ".txt", null, null, "SSSSS-of-NNNNN", StringUtf8Coder.of());
    PDone done = pKV2.apply("WriteResults", com.google.cloud.dataflow.sdk.io.Write.to(my));

    p.run();
  }
}