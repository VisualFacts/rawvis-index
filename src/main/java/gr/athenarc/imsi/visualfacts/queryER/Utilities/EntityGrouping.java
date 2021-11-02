package gr.athenarc.imsi.visualfacts.queryER.Utilities;

import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.VizCluster;
import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.VizData;
import gr.athenarc.imsi.visualfacts.queryER.DeduplicationExecution;
import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.DedupVizOutput;
import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.VizStatistic;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * @author bstam
 * Utility functions to merge an enumerable and a reverse UnionFind into merged entities.
 */
public class EntityGrouping {

 
    public static DedupVizOutput groupSimilar(HashMap<Long, Set<Long>> revUF,
                                              HashMap<Long, Object[]> newData, HashMap<Long, HashMap<Long, Double>> similarities) {

        List<Object[]> finalData = new ArrayList<>();
        Set<Long> checked = new HashSet<>();
        List<VizCluster> VizDataset = new ArrayList<>();
        List<HashMap<Integer, Double>> columnSimilarities = new ArrayList<>(); // List of the column similarities of each cluster
        int noOfFields = DeduplicationExecution.noOfFields;
        revUF.values().removeIf(v -> v.size() == 0);

        for (long id : revUF.keySet()) {
            List<VizData> entityGroup = new ArrayList<>();
            Set<Long> similar = revUF.get(id);
            /* Because we resolve all duplicates when found the first id of the cluster we use this set */
            if (checked.contains(id)) continue;
            checked.addAll(similar);
            HashMap<Integer, Double> clusterColumnSimilarity = new HashMap<>(); // This cluster's column similarities
            LinkedHashMap<Integer, HashMap<String, Integer>> clusterColumnValues = new LinkedHashMap<>(); // Columns of this cluster
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
                		HashMap<String, Integer> valueFrequencies = clusterColumnValues.computeIfAbsent(i, x -> new HashMap<>());
                		int valueFrequency = valueFrequencies.containsKey(value) ? valueFrequencies.get(value) : 0;
                		if (!value.equals("") && !datum[i].equals("[\\W_]"))
                			valueFrequencies.put(value, valueFrequency + 1);
                	}
                    entityGroup.add(new VizData(idInner, columns));
                }
            }
            if(entityGroup.size() > 1) {
	            Object[] groupedObject = clusterToString(clusterColumnValues); // Creates the grouped object from the columns map
                VizCluster cluster = new VizCluster(entityGroup, clusterColumnValues, groupedObject);
	            VizDataset.add(cluster);
	            finalData.add(groupedObject);
            }
            
        }
        double percentOfDups = (double) finalData.size() / (double)  revUF.size();
        VizStatistic VizStatistic = generateVizStatistic(VizDataset, columnSimilarities, percentOfDups);
        DedupVizOutput vizOutput = new DedupVizOutput(VizDataset, VizStatistic);
        return vizOutput;
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
        return columnValues;

    }

    private static VizStatistic generateVizStatistic(List<VizCluster> VizDataset,
                                                           List<HashMap<Integer, Double>> columnSimilarities,
                                                           double percentOfDups) {

        VizStatistic VizStatistic = new VizStatistic(percentOfDups);
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
