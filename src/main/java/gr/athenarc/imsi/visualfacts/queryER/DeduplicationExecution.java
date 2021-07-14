package gr.athenarc.imsi.visualfacts.queryER;

import gr.athenarc.imsi.visualfacts.query.QueryBlockIndex;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.DecomposedBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.EntityResolvedTuple;
import gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import gr.athenarc.imsi.visualfacts.queryER.MetaBlocking.BlockFiltering;
import gr.athenarc.imsi.visualfacts.queryER.MetaBlocking.EfficientEdgePruning;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.DumpDirectories;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.ExecuteBlockComparisons;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.MapUtilities;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.RandomAccessReader;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * Single table deduplication execution.
 */
public class DeduplicationExecution<T> {
    
    /**
     * Performs deduplication on a single table's entities.
     * The steps for performing the resolution are as follows:
     * 1) Get filtered data from filter.
     * 2) Create QueryBlockIndex
     * 3) BlockJoin
     * 4) Apply MetaBlocking
     * 5) Create an enumberable by index scanning with the IDs as derived from the MetaBlocking
     * 6) Execute Block Comparisons to find matches
     *
     * @param enumerable The data after the filter
     * @param tableName  Name of the table used to get the BlockIndex
     * @param key        Key column id of the table for block indexing
     * @param source     Source of the table data for the scan
     * @param fieldTypes Types of the data
     * @param ab         Used for the csv enumerator, nothing else
     * @return EntityResolvedTuple contains the UnionFind + HashMap of the table to be used in merging/join
     * @throws IOException
     */




    public static EntityResolvedTuple deduplicate(List<AbstractBlock> blocks, HashMap<Long, Object[]> queryData, Set<Long> qIds, String tableName, int noOfAttributes) {
    	
    	boolean firstDedup = false;
        // Check for links and remove qIds that have links
        HashMap<Long, Set<Long>> links = loadLinks(tableName);
        HashMap<Long, Object[]> dataWithLinks = new HashMap<>();
        if(links == null) firstDedup = true;
        Set<Long> totalIds = new HashSet<>();
        
        /* If there are links then we get all ids that are in the links HashMap (both on keys and the values).
         * Then we get all these data and put it onto the dataWithLinks hashMap.
         * Now we have two hashmaps 1) dataWithLinks, queryData = data without links.
         * After we deduplicate queryData, we will merge these two tables.
         */
        if(!firstDedup) {
            // Clear links and keep only qIds
        	Set<Long> linkedIds = getLinkedIds(links,  qIds); // Get extra Link Ids that are not in queryData
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
            if(tableName.contains("publications")) filterParam = 0.55;
	        BlockFiltering bFiltering = new BlockFiltering(filterParam);
	        bFiltering.applyProcessing(blocks);
            
            // EDGE PRUNING
            EfficientEdgePruning eEP = new EfficientEdgePruning();
            eEP.applyProcessing(blocks);
            epFlag = true;
            
            
        }

        //Get ids of final entities, and add back qIds that were cut from m-blocking
        Set<Long> blockQids = new HashSet<>();
        if(epFlag)
        	blockQids = QueryTokenMap.blocksToEntitiesD(blocks);
        else
        	blockQids = QueryTokenMap.blocksToEntities(blocks);
        totalIds.addAll(blockQids);
        totalIds.addAll(qIds);

        RandomAccessReader randomAccessReader = null;
        try {
        	randomAccessReader = RandomAccessReader.open(new File(""));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
        
        // Merge queryData with dataWithLinks
        queryData = mergeMaps(queryData, dataWithLinks);
        ExecuteBlockComparisons<?> ebc = new ExecuteBlockComparisons(queryData, randomAccessReader);
        EntityResolvedTuple<?> entityResolvedTuple = ebc.comparisonExecutionAll(blocks, qIdsNoLinks, noOfAttributes);
        entityResolvedTuple.mergeLinks(links, tableName, firstDedup, totalIds);

   
        // Log everything
      
        return entityResolvedTuple;
		
	}

    public static HashMap<Long, Set<Long>> loadLinks(String table) {
    	DumpDirectories dumpDirectories = new DumpDirectories();
    	if(new File(dumpDirectories.getLinksDirPath() + table).exists())
    		return (HashMap<Long, Set<Long>>) SerializationUtilities.loadSerializedObject(dumpDirectories.getLinksDirPath() + table);
    	else  return null;
    }
    
	private static HashMap<Long, Object[]> getExtraData(HashMap<Long, Object[]> dataWithLinks, Set<Long> linkedIds, HashMap<Long, Object[]> queryData) {
        
		return mergeMaps(dataWithLinks, queryData);
	}

	public static Set<Long> getLinkedIds(Map<Long, Set<Long>> links, Set<Long> qIds) {

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


	private static HashMap<Long, Object[]> mergeMaps(HashMap<Long, Object[]> queryData, HashMap<Long, Object[]> map2){
		queryData.forEach(
        	    (key, value) -> map2.put(key, value)
        	);
        return map2;
	}

    /**
     * @param <T>
     * @param entityResolvedTuple The tuple as created by the deduplication/join
     * @return 
     * @return AbstractEnumerable that combines the hashmap and the UnionFind to create the merged/fusioned data
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static EntityResolvedTuple mergeEntities(EntityResolvedTuple entityResolvedTuple, List<Integer> projects, List<String> fieldNames) {
    	entityResolvedTuple.groupEntities(projects, fieldNames);
        return entityResolvedTuple;

    }
    

}