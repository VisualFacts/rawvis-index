package gr.athenarc.imsi.visualfacts.queryER;

import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.config.ERConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class QueryTokenMap {
    private static final Logger LOG = LogManager.getLogger(QueryTokenMap.class);

    Map<String, Set<String>> map = new HashMap<>();
    HashMap<String, Integer> tfIdf = new HashMap<>();

    Schema schema;

    public QueryTokenMap(Schema schema) {
        this.schema = schema;
    }

    public Map<String, Set<String>> joinTokenMap() {
		TokenMap tokenMap = new TokenMap(this.schema);
		Map<String, Set<String>> globalTokenMap = tokenMap.map;
		Predicate<Map.Entry<String, Set<String>>> predicate = entry -> map.keySet().contains(entry.getKey()); // note this is java.util.function.Predicate
		Map<String, Set<String>> extendedQueryMap = globalTokenMap.entrySet().stream().filter(predicate)
		        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		return extendedQueryMap;
    }
    
    public void processQueryObject(String[] object) {
        for (CategoricalColumn categoricalColumn : schema.getCategoricalColumns()) {
            String value = object[categoricalColumn.getIndex()];
            if (value == null)
                continue;
            String cleanValue = value.replaceAll("_", " ").trim().replaceAll("\\s*,\\s*$", "")
                    .toLowerCase();
            for (String token : cleanValue.split("[\\W_]")) {
                if (2 < token.trim().length()) {
                    if (ERConfig.getStopwords().contains(token.toLowerCase()))
                        continue;
                    Set<String> values = map.computeIfAbsent(token.trim(),
                            x -> new HashSet<>());

                    values.add(value);
                    tfIdf.merge(token, 1, Integer::sum);
                }
            }
        }
    }
}