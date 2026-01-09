package gr.athenarc.imsi.visualfacts.util.csv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks comparing the Univocity-backed reader with the
 * Zsv-based custom CSV float row reader implementation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 0, time = 1)
@Measurement(iterations = 3, time = 1)
@org.openjdk.jmh.annotations.Fork(2)
public class CsvRowReaderBenchmark {

    @State(Scope.Thread)
    public static class ReaderState {
        Path file = Paths.get("/home/stavmars/data/data_10_cols.csv");
        CsvReaderConfig config = new CsvReaderConfig(
            file.toFile(),
            StandardCharsets.UTF_8,
                new int[] { 1, 2, 4, 9 },
                false,
            ',');
    }

    @Benchmark
    public long benchmarkUnivocity(ReaderState state) throws IOException {
        return runReader(new UnivocityCsvFloatRowReader(), state);
    }

    @Benchmark
    public long benchmarkZsvReader(ReaderState state) throws IOException {
        return runReader(new ZsvCsvFloatRowReader(), state);
    }

    private long runReader(CsvFloatRowReader reader, ReaderState state) throws IOException {
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
