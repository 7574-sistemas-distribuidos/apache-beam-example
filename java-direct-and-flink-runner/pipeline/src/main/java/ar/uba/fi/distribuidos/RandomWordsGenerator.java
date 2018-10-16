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
import org.apache.beam.sdk.io.*;
import java.util.List;
import org.joda.time.Instant;
import org.apache.beam.sdk.coders.*;
import java.io.*;
import java.util.NoSuchElementException;
import java.util.*;
import org.apache.beam.sdk.transforms.windowing.*;
import static com.google.common.base.Preconditions.*;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

class EmptyCheckpointMark implements UnboundedSource.CheckpointMark {

    private static final EmptyCheckpointMark instance = new EmptyCheckpointMark();
    private EmptyCheckpointMark() {
    }

    public static EmptyCheckpointMark get() {
        return instance;
    }

    @Override
    public void finalizeCheckpoint() {
        //do nothing
    }
}


public class RandomWordsGenerator extends PTransform<PBegin, PCollection<String>> {

    @Override
    public PCollection<String> expand(PBegin input) {
        return input.getPipeline().apply(
            Read.from(new WordsSource()));
    }

    @Override
    protected Coder<String> getDefaultOutputCoder() {
        return StringUtf8Coder.of();
    }   

    @Override
    public String getKindString() {
        return "Read(RandomWords)";
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
         builder.add(DisplayData.item("source-type", "String"));
    }

    public class WordsSource extends UnboundedSource<String, EmptyCheckpointMark> {

        @Override
        public void validate() {
            //do nothing
        }

        @Override
        public List<WordsSource> split(int desiredNumSplits, PipelineOptions options) throws Exception {
            List<WordsSource> splits = new ArrayList<>();
            for (int i = 0; i < desiredNumSplits; ++i) {
                splits.add(new WordsSource());  
            }
            return splits;
        }

        @Override
        public Reader createReader(PipelineOptions options, EmptyCheckpointMark checkpoint)
        throws IOException {
            return new Reader(options);
        }

        @Override
        public Coder<String> getDefaultOutputCoder() {
            return StringUtf8Coder.of();
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public Coder<EmptyCheckpointMark> getCheckpointMarkCoder() {
            return null;
        }

        public Reader from(PipelineOptions options) {
            return new Reader(options);
        }


        public class Reader extends UnboundedReader<String> {
            private static final String WORDS = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequait Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

            private final String[] words = WORDS.split(" ");
            private boolean done;
            private Integer wordsQty;
            private int elementsGenerated;
            private Random rand = new Random();

            public Reader(PipelineOptions options) {
                checkNotNull(options, "options");
                this.wordsQty = (options.as(WordCountPipeline.Options.class).getWordsQty());
                checkNotNull(wordsQty, "wordsQty");
                this.done = false;
                this.elementsGenerated = 0;
            }
            

            @Override
            public WordsSource getCurrentSource() {
                return WordsSource.this;
            }

            @Override
            public boolean start() throws IOException {
                return advance();
            }

            @Override
            public boolean advance() throws IOException {
                if (elementsGenerated < wordsQty) {
                    ++this.elementsGenerated;
                    return true;
                } else {
                    done = true;
                    return false;
                }
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public String getCurrent() throws NoSuchElementException {
                String word = words[rand.nextInt(words.length)];
                if (rand.nextInt(2) == 1)
                    word += "_" + rand.nextInt(10);
                return word;
            }

            @Override
            public Instant getCurrentTimestamp() throws NoSuchElementException {
                return Instant.now();
            }

            @Override
            public Instant getWatermark() {
                return done ? BoundedWindow.TIMESTAMP_MAX_VALUE : BoundedWindow.TIMESTAMP_MIN_VALUE;
            }

            @Override
            public EmptyCheckpointMark getCheckpointMark() {
                return EmptyCheckpointMark.get();
            }
        }
    }
}

