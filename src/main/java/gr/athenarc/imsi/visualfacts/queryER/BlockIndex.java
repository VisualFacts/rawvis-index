package gr.athenarc.imsi.visualfacts.queryER;

import gr.athenarc.imsi.visualfacts.Point;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.config.ERConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BlockIndex {
    private static final Logger LOG = LogManager.getLogger(BlockIndex.class);

    public Map<String, Set<Point>> invertedIndex = new HashMap<>();

    Schema schema;

    public BlockIndex(Schema schema) {
        this.schema = schema;
    }

    public void processRow(Point point, String[] row) {
        processRow(point, parseRowTokens(row));
    }

    public void processRow(Point point, Set<String> tokens) {
        for (String token : tokens) {
            Set<Point> values = invertedIndex.computeIfAbsent(token,
                    x -> new HashSet<>());
            values.add(point);
        }
    }

    public Set<String> parseRowTokens(String[] row) {
        Set<String> tokens = new HashSet<>();

        for (Integer col : schema.getBlockingCols()) {
            String value = row[col];
            if (value == null)
                continue;
            String cleanValue = value.replaceAll("_", " ").trim().replaceAll("\\s*,\\s*$", "")
                    .toLowerCase();
            for (String token : cleanValue.split("[\\W_]")) {
                if (2 < token.trim().length()) {
                    if (ERConfig.getStopwords().contains(token.toLowerCase()))
                        continue;
                    tokens.add(token.trim());
                }
            }
        }
        return tokens;
    }

    public Map<String, Set<Point>> getInvertedIndex() {
        return invertedIndex;
    }
}