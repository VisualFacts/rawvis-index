package gr.athenarc.imsi.visualfacts.queryER;

import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.EntityResolvedTuple;
import gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import gr.athenarc.imsi.visualfacts.queryER.MetaBlocking.BlockFiltering;
import gr.athenarc.imsi.visualfacts.queryER.MetaBlocking.EfficientEdgePruning;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.ExecuteBlockComparisons;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.MapUtilities;
import gr.athenarc.imsi.visualfacts.util.RawFileService;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * Single table deduplication execution.
 */
public class DeduplicationExecution<T> {

    private HashMap<Long, Set<Long>> links = new HashMap<>();

    public EntityResolvedTuple deduplicate(List<AbstractBlock> blocks, 
    		HashMap<Long, Object[]> queryData, Set<Long> qIds, String tableName, 
    		int noOfAttributes, RawFileService rawFileService) {


        boolean firstDedup = false;
        // Check for links and remove qIds that have links
        HashMap<Long, Object[]> dataWithLinks = new HashMap<>();
        if (links == null) firstDedup = true;
        Set<Long> totalIds = new HashSet<>();

        /* If there are links then we get all ids that are in the links HashMap (both on keys and the values).
         * Then we get all these data and put it onto the dataWithLinks hashMap.
         * Now we have two hashmaps 1) dataWithLinks, queryData = data without links.
         * After we deduplicate queryData, we will merge these two tables.
         */
        if (!firstDedup) {
            // Clear links and keep only qIds
            Set<Long> linkedIds = getLinkedIds(links, qIds); // Get extra Link Ids that are not in queryData
            dataWithLinks = (HashMap<Long, Object[]>) links.keySet().stream()
                    .filter(queryData::containsKey)
                    .collect(Collectors.toMap(Function.identity(), queryData::get));
            dataWithLinks = getExtraData(dataWithLinks, linkedIds, queryData);
            queryData.keySet().removeAll(links.keySet());
            totalIds.addAll(linkedIds);  // Add links back

        }
        final Set<Long> qIdsNoLinks = MapUtilities.deepCopySet(queryData.keySet());

        // PURGING 

        ComparisonsBasedBlockPurging blockPurging = new ComparisonsBasedBlockPurging();
        blockPurging.applyProcessing(blocks);


        boolean epFlag = false;
        if (blocks.size() > 10) {

            // FILTERING
            double filterParam = 0.35;
            if (tableName.contains("publications")) filterParam = 0.55;
            BlockFiltering bFiltering = new BlockFiltering(filterParam);
            bFiltering.applyProcessing(blocks);

            // EDGE PRUNING
            EfficientEdgePruning eEP = new EfficientEdgePruning();
            eEP.applyProcessing(blocks);
            epFlag = true;


        }

        //Get ids of final entities, and add back qIds that were cut from m-blocking
        Set<Long> blockQids = new HashSet<>();
        if (epFlag)
            blockQids = QueryTokenMap.blocksToEntitiesD(blocks);
        else
            blockQids = QueryTokenMap.blocksToEntities(blocks);
        totalIds.addAll(blockQids);
        totalIds.addAll(qIds);

        // Merge queryData with dataWithLinks
        queryData = mergeMaps(queryData, dataWithLinks);
        ExecuteBlockComparisons<?> ebc = new ExecuteBlockComparisons(queryData, rawFileService);
        EntityResolvedTuple<?> entityResolvedTuple = ebc.comparisonExecutionAll(blocks, qIdsNoLinks, noOfAttributes);
        this.links = entityResolvedTuple.mergeLinks(links, firstDedup, totalIds);

        // Log everything

        return entityResolvedTuple;

    }

    private HashMap<Long, Object[]> getExtraData(HashMap<Long, Object[]> dataWithLinks, Set<Long> linkedIds, HashMap<Long, Object[]> queryData) {

        return mergeMaps(dataWithLinks, queryData);
    }

    public Set<Long> getLinkedIds(Map<Long, Set<Long>> links, Set<Long> qIds) {

        Set<Long> linkedIds = new HashSet<>();
        Set<Set<Long>> sublinks = links.entrySet().stream().filter(entry -> {
            return qIds.contains(entry.getKey());
        }).map(entry -> {
            return entry.getValue();
        }).collect(Collectors.toSet());
        for (Set<Long> sublink : sublinks) {
            linkedIds.addAll(sublink);
        }
        linkedIds.removeAll(qIds);

        return linkedIds;
    }


    private HashMap<Long, Object[]> mergeMaps(HashMap<Long, Object[]> queryData, HashMap<Long, Object[]> map2) {
        queryData.forEach(
                (key, value) -> map2.put(key, value)
        );
        return map2;
    }

    /**
     * @param <T>
     * @param entityResolvedTuple The tuple as created by the deduplication/join
     * @return AbstractEnumerable that combines the hashmap and the UnionFind to create the merged/fusioned data
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public EntityResolvedTuple mergeEntities(EntityResolvedTuple entityResolvedTuple, List<Integer> projects, List<String> fieldNames) {
        entityResolvedTuple.groupEntities(projects, fieldNames);
        return entityResolvedTuple;
    }


}