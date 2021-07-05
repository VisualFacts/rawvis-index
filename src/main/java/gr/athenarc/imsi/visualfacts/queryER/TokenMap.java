package gr.athenarc.imsi.visualfacts.queryER;

import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TokenMap {
    private static final Logger LOG = LogManager.getLogger(TokenMap.class);

    Map<String, Set<String>> map = new HashMap<>();
    HashMap<String, Integer> tfIdf = new HashMap<>();

    HashSet<String> stopwords;
    Schema schema;

    public TokenMap(Schema schema) throws IOException, ClassNotFoundException {
        this.schema = schema;
        ObjectInput input = new ObjectInputStream(new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("stopwords_SER")));
        try {
            stopwords = (HashSet<String>) input.readObject();
        } finally {
            input.close();
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
                    if (stopwords.contains(token.toLowerCase()))
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