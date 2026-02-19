package gr.athenarc.imsi.visualfacts.util.csv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks comparing the Univocity-backed reader with the
 * Zsv-based custom CSV double row reader implementation.
 * 
 * For cold disk measurements, run with:
 *   java -Dbenchmark.dropCaches=true -Dcsv.path=/path/to/file.csv -jar benchmarks.jar
 * 
 * Requires passwordless sudo for cache drop. Add to /etc/sudoers:
 *   username ALL=(ALL) NOPASSWD: /usr/bin/sync, /bin/sh -c echo 3 > /proc/sys/vm/drop_caches
 */
@BenchmarkMode(Mode.SingleShotTime)  // Single shot for cold measurement
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 0)              // No warmup for cold measurement
@Measurement(iterations = 3)         // 3 cold iterations (cache dropped before each)
@org.openjdk.jmh.annotations.Fork(value = 1, warmups = 0)
public class CsvRowReaderBenchmark {

    private static final Logger LOG = LogManager.getLogger(CsvRowReaderBenchmark.class);

    @State(Scope.Thread)
    public static class ReaderState {
        Path file;
        CsvReaderConfig config;
        boolean dropCaches;

        public ReaderState() {
            String csvPathStr = System.getProperty("csv.path");
            if (csvPathStr == null) {
                try {
                    file = Paths.get(CsvRowReaderBenchmark.class.getClassLoader()
                        .getResource("data/data_10_cols_100K.csv").toURI());
                    LOG.info("Using default benchmark CSV: data/data_10_cols_100K.csv");
                } catch (Exception e) {
                    throw new RuntimeException("Default CSV resource not found", e);
                }
            } else {
                file = Paths.get(csvPathStr);
                LOG.info("Using CSV from system property: {}", csvPathStr);
            }
            config = new CsvReaderConfig(
                file.toFile(),
                StandardCharsets.UTF_8,
                new int[] { 1, 2, 4, 5 },
                false,
                ',');
            
            // Enable cache drop for cold disk measurements (requires sudo without password)
            dropCaches = Boolean.getBoolean("benchmark.dropCaches");
            if (dropCaches) {
                LOG.info("Cache drop enabled - will drop page cache before each iteration");
            }
        }

        /**
         * Drops OS page cache before each iteration for cold disk measurements.
         * Requires passwordless sudo for: sync, sh -c 'echo 3 > /proc/sys/vm/drop_caches'
         * 
         * To enable: add -Dbenchmark.dropCaches=true
         * To setup passwordless sudo, add to /etc/sudoers:
         *   username ALL=(ALL) NOPASSWD: /usr/bin/sync, /bin/sh -c echo 3 > /proc/sys/vm/drop_caches
         */
        @Setup(Level.Iteration)
        public void dropPageCache() {
            if (!dropCaches) {
                return;
            }
            try {
                LOG.info("Dropping page cache for cold disk measurement...");
                
                // Sync to flush pending writes
                Process sync = Runtime.getRuntime().exec(new String[]{"sudo", "sync"});
                int syncExit = sync.waitFor();
                if (syncExit != 0) {
                    LOG.warn("sync failed with exit code {}", syncExit);
                }
                
                // Drop page cache
                Process drop = Runtime.getRuntime().exec(new String[]{
                    "sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"
                });
                int dropExit = drop.waitFor();
                if (dropExit != 0) {
                    LOG.warn("drop_caches failed with exit code {} - cold measurements may be inaccurate", dropExit);
                }
                
                // Brief pause to ensure cache is cleared
                Thread.sleep(500);
                
                LOG.info("Page cache dropped successfully");
            } catch (Exception e) {
                LOG.warn("Failed to drop page cache: {} - cold measurements may be inaccurate", e.getMessage());
            }
        }
    }

    @Benchmark
    public long benchmarkUnivocity(ReaderState state) throws IOException {
        return runReader(new UnivocityCsvDoubleRowReader(), state);
    }

    @Benchmark
    public long benchmarkZsvReader(ReaderState state) throws IOException {
        return runReader(new ZsvCsvDoubleRowReader(), state);
    }

    private long runReader(CsvDoubleRowReader reader, ReaderState state) throws IOException {
        reader.open(state.config);
        double[] row;
        double sum = 0;
        long rows = 0;
        try {
            while ((row = reader.nextRow()) != null) {
                rows++;
                for (double v : row) {
                    sum += v;
                }
            }
        } finally {
            reader.close();
        }
        LOG.info("Sum of all values for reader {}: {}", reader.getClass().getSimpleName(), sum);
        return rows;
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
