package gr.athenarc.imsi.visualfacts.queryER.Utilities;

import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.Comparison;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.EntityResolvedTuple;
import gr.athenarc.imsi.visualfacts.util.RawFileService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExecuteBlockComparisons<T> {

    private HashMap<Long, Object[]> newData;
    private Integer noOfFields;
    private RawFileService rawFileService;
	public static Set<String> matches;


	public ExecuteBlockComparisons(HashMap<Long, Object[]> queryData, RawFileService rawFileService) { 
        this.newData = queryData;
        this.rawFileService = rawFileService;
    }

	public EntityResolvedTuple comparisonExecutionAll(List<AbstractBlock> blocks, Set<Long> qIds, Set<Integer> dedupColumns) {
        return comparisonExecutionJdk(blocks, qIds, dedupColumns);
	}


    @SuppressWarnings({"rawtypes", "unchecked"})
    public EntityResolvedTuple comparisonExecutionJdk(List<AbstractBlock> blocks, Set<Long> qIds, Set<Integer> dedupColumns) {
        int comparisons = 0;
        UnionFind uFind = new UnionFind(qIds);

        Set<AbstractBlock> nBlocks = new HashSet<>(blocks);
        Set<String> uComparisons = new HashSet<>();

        this.noOfFields = noOfFields;
        double compTime = 0.0;
        matches = new HashSet<>();
        for (AbstractBlock block : nBlocks) {
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                long id1 = comparison.getEntityId1();
                long id2 = comparison.getEntityId2();
                if (!qIds.contains(id1) && !qIds.contains(id2))
                    continue;

                String uniqueComp = "";
                if (comparison.getEntityId1() > comparison.getEntityId2())
                    uniqueComp = id1 + "u" + id2;
                else
                    uniqueComp = id2 + "u" + id1;
                if (uComparisons.contains(uniqueComp))
                    continue;
                uComparisons.add(uniqueComp);

                Object[] entity1 = getEntity(id1);
                Object[] entity2 = getEntity(id2);

                double compStartTime = System.currentTimeMillis();
                double similarity = ProfileComparison.getJaroSimilarity(entity1, entity2, dedupColumns);
                double compEndTime = System.currentTimeMillis();
                compTime += compEndTime - compStartTime;
                comparisons++;
                if (similarity >= 0.85) {
                    matches.add(uniqueComp);
                    uFind.union(id1, id2);
                }
            }
        }

        EntityResolvedTuple eRT = new EntityResolvedTuple(newData, uFind);
        eRT.setComparisons(comparisons);
        eRT.setMatches(matches.size());
        eRT.setCompTime(compTime / 1000);
        eRT.getAll();

        return eRT;
    }


    private Object[] getEntity(long id) {
        try {
            if (newData.containsKey(id)) return newData.get(id);
            Object[] entity = rawFileService.getObject(id);
            if (entity != null) {
                newData.put(id, entity);
                return entity;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Object[] emptyVal = new Object[noOfFields];
        for (int i = 0; i < noOfFields; i++) emptyVal[i] = "";
        return emptyVal;
    }

}