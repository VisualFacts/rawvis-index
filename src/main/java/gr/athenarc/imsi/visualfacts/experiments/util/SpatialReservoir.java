package gr.athenarc.imsi.visualfacts.experiments.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gr.athenarc.imsi.visualfacts.DataValidationFilter;
import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.util.csv.CsvReaderConfig;
import gr.athenarc.imsi.visualfacts.util.csv.UnivocityCsvDoubleRowReader;

/**
 * A deterministic spatial sample of (x, y) points drawn from a dataset using
 * Algorithm-R reservoir sampling over a single sequential CSV pass.
 *
 * <p>The reservoir is the substrate for the random
 * workload generator: query centres are picked from the reservoir (so the
 * query distribution follows the data distribution) and the calibrated
 * extent is estimated from reservoir-vs-reservoir mean selectivity.
 *
 * <p>Reservoirs are cached on disk under
 * {@code experiments/query_sequences/reservoirs/<dataset>_R<size>_seed<seed>.csv}
 * and reused across runs and across systems so all competitors see identical
 * query streams.
 */
public final class SpatialReservoir {

    private static final Logger LOG = LogManager.getLogger(SpatialReservoir.class);

    private final double[] xs;
    private final double[] ys;
    private final long rowsScanned;
    private final long rowsAccepted;
    private final int reservoirSize;
    private final long seed;

    private SpatialReservoir(double[] xs, double[] ys,
                             long rowsScanned, long rowsAccepted,
                             int reservoirSize, long seed) {
        this.xs = xs;
        this.ys = ys;
        this.rowsScanned = rowsScanned;
        this.rowsAccepted = rowsAccepted;
        this.reservoirSize = reservoirSize;
        this.seed = seed;
    }

    public int size() { return xs.length; }
    public double[] xs() { return xs; }
    public double[] ys() { return ys; }
    public long rowsScanned() { return rowsScanned; }
    public long rowsAccepted() { return rowsAccepted; }

    /**
     * Loads the reservoir from {@code cacheFile} if present; otherwise builds
     * it via a single sequential CSV pass and writes it to {@code cacheFile}.
     */
    public static SpatialReservoir loadOrBuild(Schema schema, String datasetName,
                                               Path cacheDir, int reservoirSize, long seed)
            throws IOException {
        Files.createDirectories(cacheDir);
        Path cacheFile = cacheDir.resolve(
            String.format("%s_R%d_seed%d.csv", datasetName, reservoirSize, seed));
        if (Files.isRegularFile(cacheFile)) {
            LOG.info("Loading cached spatial reservoir for '{}' from {}", datasetName, cacheFile);
            return readCsv(cacheFile);
        }
        LOG.info("Building spatial reservoir for '{}' (R={}, seed={}) from {}",
                datasetName, reservoirSize, seed, schema.getCsv());
        SpatialReservoir reservoir = build(schema, reservoirSize, seed);
        reservoir.writeCsv(cacheFile);
        LOG.info("Spatial reservoir for '{}': scanned {} rows, accepted {}, kept {} -> {}",
                datasetName, reservoir.rowsScanned, reservoir.rowsAccepted,
                reservoir.size(), cacheFile);
        return reservoir;
    }

    /**
     * Performs a single sequential CSV pass and returns a reservoir of
     * {@code reservoirSize} (x, y) points sampled uniformly at random from
     * rows that pass the schema's validation filters and lie within the
     * schema bounds.
     */
    public static SpatialReservoir build(Schema schema, int reservoirSize, long seed)
            throws IOException {
        if (reservoirSize <= 0) {
            throw new IllegalArgumentException("reservoirSize must be > 0");
        }

        // Resolve selected columns: x, y, plus any filter columns.
        HashSet<Integer> colIndexes = new java.util.LinkedHashSet<>();
        colIndexes.add(schema.getxColumn());
        colIndexes.add(schema.getyColumn());
        List<DataValidationFilter> filters = schema.getValidationFilters();
        if (filters != null) {
            for (DataValidationFilter f : filters) {
                colIndexes.add(f.getFilterColumn());
            }
        }
        int[] selectedColumns = colIndexes.stream().mapToInt(Integer::intValue).toArray();

        Map<Integer, Integer> col2pos = new HashMap<>();
        for (int i = 0; i < selectedColumns.length; i++) {
            col2pos.put(selectedColumns[i], i);
        }
        int xPos = col2pos.get(schema.getxColumn());
        int yPos = col2pos.get(schema.getyColumn());
        int filterCount = filters == null ? 0 : filters.size();
        int[] filterPos = new int[filterCount];
        DataValidationFilter[] filterArr = new DataValidationFilter[filterCount];
        if (filters != null) {
            for (int i = 0; i < filterCount; i++) {
                filterArr[i] = filters.get(i);
                filterPos[i] = col2pos.get(filterArr[i].getFilterColumn());
            }
        }

        Rectangle bounds = schema.getBounds();
        double xLo = bounds.getXRange().lowerEndpoint();
        double xHi = bounds.getXRange().upperEndpoint();
        double yLo = bounds.getYRange().lowerEndpoint();
        double yHi = bounds.getYRange().upperEndpoint();

        CsvReaderConfig config = new CsvReaderConfig(
                new File(schema.getCsv()), StandardCharsets.UTF_8, selectedColumns,
                schema.getHasHeader(), schema.getDelimiter(), 0L, -1L, schema.getNullstr());

        double[] xs = new double[reservoirSize];
        double[] ys = new double[reservoirSize];

        Random rng = new Random(seed);
        long rowsScanned = 0;
        long rowsAccepted = 0;
        long progressEvery = 5_000_000L;

        UnivocityCsvDoubleRowReader reader = new UnivocityCsvDoubleRowReader();
        try {
            reader.open(config);
            double[] row;
            while ((row = reader.nextRow()) != null) {
                rowsScanned++;
                if (rowsScanned % progressEvery == 0) {
                    LOG.info("  reservoir build: {} rows scanned, {} accepted",
                            rowsScanned, rowsAccepted);
                }
                if (row.length <= xPos || row.length <= yPos) continue;
                double x = row[xPos];
                double y = row[yPos];
                if (Double.isNaN(x) || Double.isNaN(y)) continue;
                if (x < xLo || x > xHi || y < yLo || y > yHi) continue;
                boolean pass = true;
                for (int i = 0; i < filterCount; i++) {
                    int p = filterPos[i];
                    if (p >= row.length || !filterArr[i].test(row[p])) {
                        pass = false;
                        break;
                    }
                }
                if (!pass) continue;

                rowsAccepted++;
                if (rowsAccepted <= reservoirSize) {
                    int idx = (int) (rowsAccepted - 1);
                    xs[idx] = x;
                    ys[idx] = y;
                } else {
                    // Algorithm R: replace slot j with prob R/n_accepted.
                    long j = (long) (rng.nextDouble() * rowsAccepted);
                    if (j < reservoirSize) {
                        int idx = (int) j;
                        xs[idx] = x;
                        ys[idx] = y;
                    }
                }
            }
        } finally {
            reader.close();
        }

        if (rowsAccepted < reservoirSize) {
            // Trim to actual size for tiny datasets.
            int n = (int) rowsAccepted;
            double[] xs2 = new double[n];
            double[] ys2 = new double[n];
            System.arraycopy(xs, 0, xs2, 0, n);
            System.arraycopy(ys, 0, ys2, 0, n);
            xs = xs2;
            ys = ys2;
        }
        return new SpatialReservoir(xs, ys, rowsScanned, rowsAccepted, reservoirSize, seed);
    }

    public void writeCsv(Path file) throws IOException {
        Files.createDirectories(file.getParent());
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write(String.format(
                "# rowsScanned=%d rowsAccepted=%d reservoirSize=%d seed=%d kept=%d%n",
                rowsScanned, rowsAccepted, reservoirSize, seed, xs.length));
            w.write("x,y\n");
            StringBuilder sb = new StringBuilder(64);
            for (int i = 0; i < xs.length; i++) {
                sb.setLength(0);
                sb.append(xs[i]).append(',').append(ys[i]).append('\n');
                w.write(sb.toString());
            }
        }
    }

    public static SpatialReservoir readCsv(Path file) throws IOException {
        long rowsScanned = 0, rowsAccepted = 0;
        int reservoirSize = 0;
        long seed = 0;
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line = r.readLine();
            if (line != null && line.startsWith("#")) {
                for (String tok : line.substring(1).trim().split("\\s+")) {
                    int eq = tok.indexOf('=');
                    if (eq < 0) continue;
                    String k = tok.substring(0, eq);
                    String v = tok.substring(eq + 1);
                    switch (k) {
                        case "rowsScanned":   rowsScanned   = Long.parseLong(v); break;
                        case "rowsAccepted":  rowsAccepted  = Long.parseLong(v); break;
                        case "reservoirSize": reservoirSize = Integer.parseInt(v); break;
                        case "seed":          seed          = Long.parseLong(v); break;
                        default: break;
                    }
                }
                line = r.readLine(); // skip "x,y" header
            }
            // Two-pass: count then read. Cheaper for tens of MB.
            java.util.ArrayList<double[]> rows = new java.util.ArrayList<>(100_000);
            String dataLine;
            while ((dataLine = r.readLine()) != null) {
                if (dataLine.isEmpty()) continue;
                int comma = dataLine.indexOf(',');
                if (comma < 0) continue;
                double x = Double.parseDouble(dataLine.substring(0, comma));
                double y = Double.parseDouble(dataLine.substring(comma + 1));
                rows.add(new double[]{x, y});
            }
            double[] xs = new double[rows.size()];
            double[] ys = new double[rows.size()];
            for (int i = 0; i < rows.size(); i++) {
                xs[i] = rows.get(i)[0];
                ys[i] = rows.get(i)[1];
            }
            return new SpatialReservoir(xs, ys, rowsScanned, rowsAccepted, reservoirSize, seed);
        }
    }
}
