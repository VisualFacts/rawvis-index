package gr.athenarc.imsi.visualfacts.tests.groundtruth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor.QueryResult;
import gr.athenarc.imsi.visualfacts.experiments.util.DuckDBQueryExecutor.StatsDuckDB;
import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.tests.util.ProgressBar;

/**
 * Exact ground-truth calculator for tests using DuckDB.
 */
public final class GroundTruthCalculator {

    private static final Logger LOG = LogManager.getLogger(GroundTruthCalculator.class);

    private GroundTruthCalculator() {
    }

    public static List<Map<Integer, StatsDuckDB>> computeAll(Schema schema, List<Query> queries) {
        if (queries == null || queries.isEmpty()) {
            return Collections.emptyList();
        }

        List<Map<Integer, StatsDuckDB>> statsList = new ArrayList<>();
        DuckDBQueryExecutor duckdb = null;
        try {
            // Format column names with proper padding
            String xCol = "column" + String.format("%02d", schema.getxColumn());
            String yCol = "column" + String.format("%02d", schema.getyColumn());
            
            duckdb = new DuckDBQueryExecutor(
                    schema.getCsv(),
                    DuckDBQueryExecutor.ExecutionMode.TABLE,
                    xCol,
                    yCol,
                    schema.getValidationFilters());
            
            int total = queries.size();
            for (int i = 0; i < total; i++) {
                QueryResult qr = duckdb.executeQuery(queries.get(i));
                statsList.add(qr.getMeasureStats());
                ProgressBar.print("Ground truth", i + 1, total);
            }
            ProgressBar.finish();
        } catch (Exception e) {
            throw new RuntimeException("DuckDB ground truth calculation failed", e);
        } finally {
            if (duckdb != null) {
                try {
                    duckdb.close();
                } catch (Exception ignore) {
                }
            }
        }
        return statsList;
    }
}
