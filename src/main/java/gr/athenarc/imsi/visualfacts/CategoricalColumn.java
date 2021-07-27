package gr.athenarc.imsi.visualfacts;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import gr.athenarc.imsi.visualfacts.query.Query;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static gr.athenarc.imsi.visualfacts.config.IndexConfig.*;

public class CategoricalColumn {
    private static final Logger LOG = LogManager.getLogger(CategoricalColumn.class);

    private int index;

    //private Map<String, Short> valueMap = new HashMap<>();
    private BiMap<String, Short> valueMap = HashBiMap.create();


    public CategoricalColumn(int index) {
        this.index = index;
    }

    public short getValueKey(String value) {
        if (value == null){
            value = "N/A";
        }

        try {
            return valueMap.computeIfAbsent(value, s -> (short) (valueMap.size()));
        } catch (Exception e){
            LOG.debug(value);
            LOG.debug(valueMap);
            return 0;
        }
    }

    public String getValue(Short key) {
        return valueMap.inverse().get(key);
    }

    public Map<String, Short> getValueMap() {
        return valueMap;
    }

    public List<String> getValues() {
        return new ArrayList<>(valueMap.inverse().values());
    }

    public List<String> getNonNullValues() {
        return new ArrayList<>(valueMap.inverse().values().stream().filter(v -> v != null && !v.isEmpty()).collect(Collectors.toList()));
    }

    public int getIndex() {
        return index;
    }

    public int getCardinality() {
        return valueMap.size();
    }

    @Override
    public String toString() {
        return "{" +
                "col=" + index +
                ", card=" + getCardinality() +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CategoricalColumn that = (CategoricalColumn) o;

        return index == that.index;
    }

    @Override
    public int hashCode() {
        return index;
    }

    public double getScore(Query q0) {
        double score;
        if (q0.getCategoricalFilters() != null && q0.getCategoricalFilters().containsKey(this.getIndex())) {
            score = FILTER_SCORE;
        } else if (q0.getGroupByCols() != null && q0.getGroupByCols().contains(this.getIndex())) {
            score = GROUP_BY_SCORE;
        } else {
            score = DEFAULT_SCORE;
        }
        return score;
    }
}
