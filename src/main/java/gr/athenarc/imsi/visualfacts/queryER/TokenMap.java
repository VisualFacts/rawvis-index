package gr.athenarc.imsi.visualfacts.queryER;

import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.config.ERConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TokenMap {
    private static final Logger LOG = LogManager.getLogger(TokenMap.class);

    public Map<Integer, Map<String, Set<String>>> map = new HashMap<>();
    HashMap<String, Integer> tfIdf = new HashMap<>();

    Schema schema;

    public TokenMap(Schema schema) {
        this.schema = schema;
        for (CategoricalColumn categoricalColumn : schema.getCategoricalColumns()){
            map.put(categoricalColumn.getIndex(), new HashMap<>());
        }
    }

    public void processRow(String[] row) {
        for (CategoricalColumn categoricalColumn : schema.getCategoricalColumns()) {
            String value = row[categoricalColumn.getIndex()];
            if (value == null)
                continue;
            String cleanValue = value.replaceAll("_", " ").trim().replaceAll("\\s*,\\s*$", "")
                    .toLowerCase();
            for (String token : cleanValue.split("[\\W_]")) {
                if (2 < token.trim().length()) {
                    if (ERConfig.getStopwords().contains(token.toLowerCase()))
                        continue;
                    Set<String> values = map.get(categoricalColumn.getIndex()).computeIfAbsent(token.trim(),
                            x -> new HashSet<>());
                    values.add(value);
                    tfIdf.merge(token, 1, Integer::sum);
                }
            }
        }
    }
}