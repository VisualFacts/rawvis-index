package gr.athenarc.imsi.visualfacts.tests.groundtruth;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.math.Stats;
import com.google.common.math.StatsAccumulator;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import gr.athenarc.imsi.visualfacts.DataValidationFilter;
import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.query.QueryResults;

/**
 * Exact ground-truth calculator for tests. Streams the CSV once and evaluates all queries.
 */
public final class GroundTruthCalculator {
    private GroundTruthCalculator() {
    }

    public static QueryResults compute(Schema schema, Query query) {
        List<QueryResults> results = computeAll(schema, Collections.singletonList(query));
        return results.isEmpty() ? new QueryResults(query) : results.get(0);
    }

    public static List<QueryResults> computeAll(Schema schema, List<Query> queries) {
        if (queries == null || queries.isEmpty()) {
            return Collections.emptyList();
        }

        List<QueryContext> contexts = new ArrayList<>();
        Set<Integer> measureColumns = new HashSet<>();
        for (Query query : queries) {
            QueryResults qr = new QueryResults(query);
            Map<Integer, StatsAccumulator> accumulators = new HashMap<>();
            for (Integer measure : query.getMeasureCols()) {
                measureColumns.add(measure);
                accumulators.put(measure, new StatsAccumulator());
            }
            contexts.add(new QueryContext(query.getRect(), qr, accumulators));
        }

        Set<Integer> selectedColumns = new HashSet<>();
        selectedColumns.add(schema.getxColumn());
        selectedColumns.add(schema.getyColumn());
        selectedColumns.addAll(measureColumns);
        for (DataValidationFilter filter : schema.getValidationFilters()) {
            selectedColumns.add(filter.getFilterColumn());
        }

        CsvParserSettings settings = schema.createCsvParserSettings();
        settings.selectIndexes(selectedColumns.toArray(new Integer[0]));
        settings.setColumnReorderingEnabled(false);
        settings.setHeaderExtractionEnabled(schema.getHasHeader());
        CsvParser parser = new CsvParser(settings);

        List<DataValidationFilter> filters = new ArrayList<>(schema.getValidationFilters());

        parser.beginParsing(new File(schema.getCsv()), Charset.forName("US-ASCII"));
        String[] row;
        while ((row = parser.parseNext()) != null) {
            try {
                if (shouldSkipRow(row, filters)) {
                    continue;
                }
                float x = Float.parseFloat(row[schema.getxColumn()]);
                float y = Float.parseFloat(row[schema.getyColumn()]);

                for (QueryContext ctx : contexts) {
                    if (!ctx.rectangle.contains(x, y)) {
                        continue;
                    }
                    for (Map.Entry<Integer, StatsAccumulator> entry : ctx.accumulators.entrySet()) {
                        String value = row[entry.getKey()];
                        if (value == null) {
                            continue;
                        }
                        entry.getValue().add(Double.parseDouble(value));
                    }
                }
            } catch (Exception ignore) {
                // Skip malformed lines to mirror engine behavior
            }
        }
        parser.stopParsing();

        List<QueryResults> results = new ArrayList<>();
        for (QueryContext ctx : contexts) {
            Map<Integer, Stats> rectStats = new HashMap<>();
            ctx.accumulators.forEach((measure, accumulator) -> rectStats.put(measure, accumulator.snapshot()));
            ctx.results.setRectStats(rectStats);
            results.add(ctx.results);
        }
        return results;
    }

    private static boolean shouldSkipRow(String[] row, List<DataValidationFilter> filters) {
        for (DataValidationFilter filter : filters) {
            int column = filter.getFilterColumn();
            try {
                Double value = Double.parseDouble(row[column]);
                if (filter.test(value)) {
                    return true;
                }
            } catch (NumberFormatException e) {
                return true;
            }
        }
        return false;
    }

    private static final class QueryContext {
        final Rectangle rectangle;
        final QueryResults results;
        final Map<Integer, StatsAccumulator> accumulators;

        QueryContext(Rectangle rectangle, QueryResults results, Map<Integer, StatsAccumulator> accumulators) {
            this.rectangle = rectangle;
            this.results = results;
            this.accumulators = accumulators;
        }
    }
}
