package gr.athenarc.imsi.visualfacts.util.csv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Benchmarks comparing the Univocity-backed reader with the custom
 * CsvRowReaderImpl
 * on the 1M-row dataset from test resources.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class CsvRowReaderBenchmark {

    @State(Scope.Thread)
    public static class ReaderState {
        Path file = Paths.get("src/test/resources/data/data_10_cols_1M.csv");
        CsvReaderConfig config = new CsvReaderConfig(
                file.toFile(),
                StandardCharsets.US_ASCII,
                null,
                false,
                ',');
    }

    @Benchmark
    public long benchmarkUnivocity(ReaderState state) throws IOException {
        return runReader(new UnivocityCsvRowReader(), state);
    }

    @Benchmark
    public long benchmarkCustomReader(ReaderState state) throws IOException {
        return runReader(new CsvRowReaderImpl(), state);
    }

    private long runReader(CsvRowReader reader, ReaderState state) throws IOException {
        reader.open(state.config);
        long rows = 0;
        try {
            while (reader.nextRow() != null) {
                rows++;
            }
        } finally {
            reader.close();
        }
        return rows;
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
