package gr.athenarc.imsi.visualfacts.queryER;

import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.EntityResolvedTuple;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.UnilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import gr.athenarc.imsi.visualfacts.queryER.MetaBlocking.BlockFiltering;
import gr.athenarc.imsi.visualfacts.queryER.MetaBlocking.EfficientEdgePruning;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.ExecuteBlockComparisons;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.LinksUtilities;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.MapUtilities;
import gr.athenarc.imsi.visualfacts.util.RawFileService;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * Single table deduplication execution.
 */
public class DeduplicationExecution<T> {

    public static Set<Long> qIds = new HashSet<>();
    public static List<AbstractBlock> blocks;
    public static int noOfFields = 0;
    
    public static int getNoOfFields(HashMap<Long, String[]> data) {
		Random generator = new Random();
		Object[] values = data.values().toArray();
		Object[] randomValue;
		do {
			randomValue = (Object[]) values[generator.nextInt(values.length)];
		}
		while(randomValue == null);
		return randomValue.length;
	}
    
    public EntityResolvedTuple deduplicate(List<AbstractBlock> blocks, LinksUtilities linksUtilities,
    		 String tableName, int noOfAttributes, RawFileService rawFileService, int key) {
    	
        
    	
        Set<Long> totalIds = linksUtilities.getTotalIds();
        Set<Long> qIdsNoLinks = linksUtilities.getTotalIds();
        HashMap<Long, String[]> dataWithLinks = linksUtilities.getDataWithLinks();
        HashMap<Long, String[]> dataWithoutLinks = linksUtilities.getDataWithoutLinks();
        DeduplicationExecution.qIds = linksUtilities.getqIdsNoLinks();
        if(dataWithoutLinks.size() == 0)
            DeduplicationExecution.noOfFields = getNoOfFields(dataWithLinks);
        else DeduplicationExecution.noOfFields = getNoOfFields(dataWithoutLinks);
        // PURGING 

        ComparisonsBasedBlockPurging blockPurging = new ComparisonsBasedBlockPurging();
        blockPurging.applyProcessing(blocks);

        
        boolean epFlag = false;
        System.out.println(blocks.size());
        if (blocks.size() > 10) {

            // FILTERING
            double filterParam = 0.35;
            BlockFiltering bFiltering = new BlockFiltering(filterParam);
            bFiltering.applyProcessing(blocks);

            // EDGE PRUNING
            double start = System.currentTimeMillis();
//            if(blocks.size() > 1000) {
//	            EfficientEdgePruning eEP = new EfficientEdgePruning();
//	            eEP.applyProcessing(blocks);
//	            epFlag = true;
//	            double end = System.currentTimeMillis();
//	            System.out.println("EP time: " + String.valueOf(end - start));
//            }

        }

        DeduplicationExecution.blocks = blocks;
        //Get ids of final entities, and add back qIds that were cut from m-blocking
        Set<Long> blockQids = new HashSet<>();
        if (epFlag)
            blockQids = QueryBlockIndex.blocksToEntitiesD(blocks);
        else
            blockQids = QueryBlockIndex.blocksToEntities(blocks);
        totalIds.addAll(blockQids);
        totalIds.addAll(qIds);
        // Merge queryData with dataWithLinks
        dataWithoutLinks = linksUtilities.mergeMaps(dataWithoutLinks, dataWithLinks);
        ExecuteBlockComparisons<?> ebc = new ExecuteBlockComparisons(dataWithoutLinks, rawFileService, key);
        double start = System.currentTimeMillis();

        EntityResolvedTuple<?> entityResolvedTuple = ebc.comparisonExecutionAll(blocks, qIdsNoLinks, noOfAttributes);
        double end = System.currentTimeMillis();
        System.out.println("comp time: " + String.valueOf(end - start));

        entityResolvedTuple.mergeLinks(linksUtilities.getLinks(), linksUtilities.isFirstDedup(), totalIds);
        
        // Log everything

        return entityResolvedTuple;

    }

    
    private List<AbstractBlock> copyBlocks(List<AbstractBlock> blocks) {
    	List<AbstractBlock> clone = new ArrayList<>();
    	for (AbstractBlock block : blocks) {
    		UnilateralBlock bu = (UnilateralBlock) block;
    		AbstractBlock c = new UnilateralBlock(bu.getEntities());
    		c.setBlockIndex(block.getBlockIndex());
    		c.setUtilityMeasure(block.getUtilityMeasure());
    		clone.add(c);
    	}
    	return clone;
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