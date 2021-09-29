package gr.athenarc.imsi.visualfacts.queryER.Utilities;

import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.VizCluster;
import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.VizData;
import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.DedupVizOutput;
import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.VizStatistic;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * @author bstam
 * Utility functions to merge an enumerable and a reverse UnionFind into merged entities.
 * @param newData 
 */
public class EntityGrouping {

	public static int getNoOfFields(HashMap<Long, Object[]> data) {
		Random generator = new Random();
		Object[] values = data.values().toArray();
		Object[] randomValue = (Object[]) values[generator.nextInt(values.length)];
		return randomValue.length;
	}
 
    public static DedupVizOutput groupSimilar(HashMap<Long, Set<Long>> revUF,
                                              HashMap<Long, Object[]> newData, HashMap<Long, HashMap<Long, Double>> similarities) {

        List<Object[]> finalData = new ArrayList<>();
        Set<Long> checked = new HashSet<>();
        List<VizCluster> VizDataset = new ArrayList<>();
        List<HashMap<Integer, Double>> columnSimilarities = new ArrayList<>(); // List of the column similarities of each cluster
        int noOfFields = getNoOfFields(newData);
        LinkedHashMap<Integer, HashMap<String, Integer>> clustersColumnValues = new LinkedHashMap<>();
        for (long id : revUF.keySet()) {
            List<VizData> entityGroup = new ArrayList<>();
            Set<Long> similar = revUF.get(id);
            /* Because we resolve all duplicates when found the first id of the cluster we use this set */
            if (checked.contains(id)) continue;
            checked.addAll(similar);
            HashMap<Integer, Double> clusterColumnSimilarity = new HashMap<>(); // This cluster's column similarities
            LinkedHashMap<Integer, HashMap<String, Integer>> clusterColumns = new LinkedHashMap<>(); // Columns of this cluster
            for (long idInner : similar) {
            	HashMap<Integer, String> columns = new HashMap<>();
                Object[] datum = newData.get(idInner);
                if (datum != null) {
                	for (int i = 0; i < noOfFields; i++) {
                		String value = "";
                		try {
                			value = datum[i].toString();
                		}
                		catch(Exception e) {}
            			columns.put(i, value); // for json
                		HashMap<String, Integer> valueFrequencies = clusterColumns.computeIfAbsent(i, x -> new HashMap<>());
                		int valueFrequency = valueFrequencies.containsKey(value) ? valueFrequencies.get(value) : 0;
                		if (!value.equals("") && !datum[i].equals("[\\W_]"))
                			valueFrequencies.put(value, valueFrequency + 1);

                		/* If there are duplicates we get the frequencies of the values of this cluster */
                		if (similar.size() > 1) {
                			HashMap<String, Integer> valueFrequenciesDup = clustersColumnValues.computeIfAbsent(i, x -> new HashMap<>());
                			int valueFrequencyDup = valueFrequenciesDup.containsKey(value) ? valueFrequenciesDup.get(value) : 0;
                			if (!value.equals("") && !datum[i].equals("[\\W_]"))
                				valueFrequenciesDup.put(value, valueFrequencyDup + 1);
                		}
                	}

                }
                entityGroup.add(new VizData(idInner, columns));
            }

            similar.remove(id);
            Object[] groupedObject = clusterToString(clusterColumns); // Creates the grouped object from the columns map
            /* If there are duplicates we compute the statistics of the cluster */
            if (similar.size() > 0) {
                clusterColumnSimilarity = (HashMap<Integer, Double>) getDistanceMeasure(clusterColumns);
                for (int i = 0; i < noOfFields; i++) clusterColumnSimilarity.putIfAbsent(i, 0.0);
                Map<Integer, HashMap<Integer, Double>> clusterSimilarities = new HashMap<>();
                columnSimilarities.add(clusterColumnSimilarity);
                if (similarities != null)
                    similar.stream()
                            .filter(similarities::containsKey)
                            .collect(Collectors.toMap(Function.identity(), similarities::get));
                VizCluster cluster = new VizCluster(entityGroup, clusterColumnSimilarity, clusterColumns, clusterSimilarities, groupedObject);
                VizDataset.add(cluster);

            }
            finalData.add(groupedObject);
        }
        revUF.clear();
        newData.clear();
        VizStatistic VizStatistic = generateVizStatistic(VizDataset, columnSimilarities, clustersColumnValues, finalData.size());
        DedupVizOutput vizOutput = new DedupVizOutput(VizDataset, VizStatistic);
        return vizOutput;
    }

    static Map<Integer, Double> getDistanceMeasure(LinkedHashMap<Integer, HashMap<String, Integer>> clusterColumns) {

        Map<Integer, Double> distMeasures = clusterColumns.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> {
                    Object[] keys = e.getValue().keySet().toArray();
                    return elementWiseJaro(keys);
                }));
        return distMeasures;

    }

    static Double elementWiseJaro(Object[] vals) {
        double avg = 0.0;
        int size = vals.length;
        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++) {
                avg += ProfileComparison.jaro(vals[i].toString(), vals[j].toString());
            }
        }
        return avg / size;
    }

    static Object[] clusterToString(LinkedHashMap<Integer, HashMap<String, Integer>> clusterColumns) {
        Object[] columnValues = clusterColumns.values().stream().map(v -> {
            Object[] keys = v.keySet().toArray();

            String colVal = "";
            int sz = keys.length;
            if (sz > 0) colVal = keys[0].toString();
            for (int i = 1; i < keys.length; i++) {
                colVal += " | " + keys[i].toString();
            }

            return colVal;
        }).toArray();
//		for(int i = 0; i < columnValues.length; i ++) System.out.print(columnValues[i] + ", ") ;
//		System.out.println();
        return columnValues;

    }

    private static VizStatistic generateVizStatistic(List<VizCluster> VizDataset,
                                                           List<HashMap<Integer, Double>> columnSimilarities,
                                                           LinkedHashMap<Integer, HashMap<String, Integer>> clustersColumnValues, int size) {
        double percentOfDups = (double) VizDataset.size() / (double) size;
        Map<Integer, Double> avgColumSimilarities = new HashMap<>();
        avgColumSimilarities = columnSimilarities.stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.averagingDouble(value -> (value.getValue()))));

        VizStatistic VizStatistic = new VizStatistic(percentOfDups, (HashMap<Integer, Double>) avgColumSimilarities, clustersColumnValues);
        return VizStatistic;
    }

    public static List<Object[]> sortSimilar(HashMap<Integer, Set<Integer>> revUF, HashMap<Integer, Object[]> newData) {
        // TODO Auto-generated method stub
        List<Object[]> finalData = new ArrayList<>();

        for (int id : revUF.keySet()) {
            for (int idInner : revUF.get(id)) {
                finalData.add(newData.get(idInner));
            }
        }
        revUF.clear();
        newData.clear();
        return finalData;
    }

}
