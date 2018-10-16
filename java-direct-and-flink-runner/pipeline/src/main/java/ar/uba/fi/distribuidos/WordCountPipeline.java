/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ar.uba.fi.distribuidos;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.joda.time.Duration;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;

public class WordCountPipeline {

    public static class AddTimestampFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.outputWithTimestamp(c.element(), Instant.now());
        }
    }

    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist = Metrics.distribution(
                ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(ProcessContext c) {
            lineLenDist.update(c.element().length());
            if (c.element().trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = c.element().split("[^a-zA-Z]");

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    c.output(word);
                }
            }
        }
    }

    public static class CountWords extends PTransform<PCollection<String>,
           PCollection<KV<String, Long>>> {
               @Override
               public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
                   // Convert lines of text into individual words.
                   PCollection<String> words = lines.apply(
                           ParDo.of(new ExtractWordsFn()));

                   // Count the number of times each word occurs.
                   PCollection<KV<String, Long>> wordCounts =
                       words.apply(Count.<String>perElement());

                   return wordCounts;
               }
    }

    /** A SimpleFunction that converts a Word and Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    /**
     * Options supported by {@link WordCountPipeline}.
     * <p>
     * Inherits standard configuration options.
     */
    public interface Options extends PipelineOptions {
        @Description("Fixed window duration, in minutes")
        @Default.Integer(10)
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("Quantity of words to generate")
        @Default.Integer(2000000)
        Integer getWordsQty();
        void setWordsQty(Integer value);

        @Description("Path of the file to write to")
        @Default.String("/tmp/output.txt")
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(Options.class);
        Duration windowDuration = Duration.standardSeconds(options.getWindowSize());

        Pipeline p = Pipeline.create(options);

        PCollection<String> input = p
            .apply("Read_Lines", new RandomWordsGenerator())
            //.apply("Read_Lines", TextIO.read().from("/tmp/input.txt"))
            .apply("Add_Timestamp", ParDo.of(new AddTimestampFn()));
        PCollection<String> windowedInput = input.apply("Apply_Window",
                Window.<String>into(FixedWindows.of(windowDuration))
                    .triggering(AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(windowDuration)))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes());
        windowedInput.apply("Count_Words", new CountWords())
            .apply("As_Text", MapElements.via(new FormatAsTextFn()))
            .apply("Write_Results", TextIO.write().to(options.getOutput())
                    .withWindowedWrites().withNumShards(1));

        p.run().waitUntilFinish();
    }
}

