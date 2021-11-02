package gr.athenarc.imsi.visualfacts.query;

import com.google.common.collect.ImmutableList;
import com.google.common.math.PairedStats;
import com.google.common.math.PairedStatsAccumulator;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Stats {

    private Map<ImmutableList<String>, PairedStatsAccumulator> groupStats = new HashMap<>();;

    private PairedStatsAccumulator rectStats = new PairedStatsAccumulator();;


    public Map<ImmutableList<String>, PairedStats> getGroupStats() {
        return groupStats.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().snapshot()));
    }

    public void add(ImmutableList<String> groupByValues, float measureValue0, float measureValue1) {
        groupStats.computeIfAbsent(groupByValues, (v) -> new PairedStatsAccumulator()).add(measureValue0, measureValue1);
        rectStats.add(measureValue0, measureValue1);
    }

    public void add(ImmutableList<String> groupByValues, PairedStats stats) {
        groupStats.computeIfAbsent(groupByValues, (v) -> new PairedStatsAccumulator()).addAll(stats);
        rectStats.addAll(stats);

    }


    public PairedStatsAccumulator getRectStats() {
        return rectStats;
    }
}
