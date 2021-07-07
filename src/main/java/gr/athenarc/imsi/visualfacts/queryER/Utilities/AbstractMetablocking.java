package gr.athenarc.imsi.visualfacts.queryER.Utilities;

import org.apache.commons.lang.ArrayUtils;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.*;
import gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.AbstractEfficiencyMethod;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.MetaBlockingConfiguration.WeightingScheme;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public abstract class AbstractMetablocking extends AbstractEfficiencyMethod {

    protected double totalBlocks;
    protected double validComparisons;
    protected double[] comparisonsPerBlock;
    protected double[] redundantCPE;
    protected double[] comparisonsPerEntity;
    
    protected EntityIndex entityIndex;
    protected final WeightingScheme weightingScheme;
    
    public AbstractMetablocking(String description, WeightingScheme scheme) {
        super(description + " + " + scheme);
        weightingScheme = scheme;
    }
    
    protected DecomposedBlock getDecomposedBlock (boolean cleanCleanER, List<Long> entities1, List<Long> entities2) {
        long[] entityIds1 = Converter.convertListToArray(entities1);
        long[] entityIds2 = Converter.convertListToArray(entities2);
        return new DecomposedBlock(cleanCleanER, entityIds1, entityIds2);
    }

    protected UnilateralBlock getUnilateralBlock (boolean cleanCleanER, List<Long> entities1, List<Long> entities2) {
    	long[] entityIds1 = Converter.convertListToArray(entities1);
    	long[] entityIds2 = Converter.convertListToArray(entities2);
    	long[] entityIds = ArrayUtils.addAll(entityIds1,entityIds2);
        return new UnilateralBlock(entityIds);
    }


    protected DecomposedBlock getDecomposedBlock (boolean cleanCleanER, Iterator<Comparison> iterator) {
        final List<Long> entities1 = new ArrayList<Long>();
        final List<Long> entities2 = new ArrayList<Long>();
        while (iterator.hasNext()) {
            Comparison comparison = iterator.next();
            entities1.add(comparison.getEntityId1());
            entities2.add(comparison.getEntityId2());
        }
        long[] entityIds1 = Converter.convertListToArray(entities1);
        long[] entityIds2 = Converter.convertListToArray(entities2);
        return new DecomposedBlock(cleanCleanER, entityIds1, entityIds2);
    }
    
    protected void getStatistics(List<AbstractBlock> blocks) {
        if (entityIndex == null) {
            entityIndex = new EntityIndex(blocks);
        }
        
        validComparisons = 0;
        totalBlocks = blocks.size();
        redundantCPE = new double[entityIndex.getNoOfEntities()];
        comparisonsPerBlock = new double[(int)(totalBlocks + 1)];
        comparisonsPerEntity = new double[entityIndex.getNoOfEntities()];
        for (AbstractBlock block : blocks) {
            comparisonsPerBlock[block.getBlockIndex()] = block.getNoOfComparisons();
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                long entityId2 = comparison.getEntityId2()+entityIndex.getDatasetLimit();
                
                redundantCPE[(int) comparison.getEntityId1()]++;
                redundantCPE[(int) entityId2]++;
                if (!entityIndex.isRepeated(block.getBlockIndex(), comparison)) {
                    validComparisons++;
                    comparisonsPerEntity[(int) comparison.getEntityId1()]++;
                    comparisonsPerEntity[(int) entityId2]++;
                }
            }
        }
    }
    
    protected void getValidComparisons(List<AbstractBlock> blocks) {
    	initializeEntityIndex(blocks);
        
        validComparisons = 0;
   
        for (AbstractBlock block : blocks) {
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();     
                if (!entityIndex.isRepeated(block.getBlockIndex(), comparison)) {
                    validComparisons++;    
                }
            }
        }
		System.out.println("Valid comps real: " + validComparisons);
    }
    
    protected void initializeEntityIndex(List<AbstractBlock> blocks) {
    	 if (entityIndex == null) {
             entityIndex = new EntityIndex(blocks);
         }
    }

    protected double getWeight(int blockIndex, Comparison comparison) {
        switch (weightingScheme) {
            case ARCS:
                final List<Long> commonIndices = entityIndex.getCommonBlockIndices(blockIndex, comparison);
                if (commonIndices == null) {
                    return -1;
                }

                double totalWeight = 0;
                for (long index : commonIndices) {
                    totalWeight += 1.0 / comparisonsPerBlock[(int) index];
                }
                return totalWeight;
            case CBS:
                return entityIndex.getNoOfCommonBlocks(blockIndex, comparison);
            case ECBS:
                double commonBlocks = entityIndex.getNoOfCommonBlocks(blockIndex, comparison);
                if (commonBlocks < 0) {
                    return commonBlocks;
                }
                return commonBlocks * Math.log10(totalBlocks / entityIndex.getNoOfEntityBlocks((int) comparison.getEntityId1(), 0)) * Math.log10(totalBlocks / entityIndex.getNoOfEntityBlocks((int) comparison.getEntityId2(), comparison.isCleanCleanER()?1:0));
            case JS:
                double commonBlocksJS = entityIndex.getNoOfCommonBlocks(blockIndex, comparison);
                if (commonBlocksJS < 0) {
                    return commonBlocksJS;
                }
                return commonBlocksJS / (entityIndex.getNoOfEntityBlocks((int) comparison.getEntityId1(), 0) + entityIndex.getNoOfEntityBlocks((int) comparison.getEntityId2(), comparison.isCleanCleanER()?1:0) - commonBlocksJS);
            case EJS:
                double commonBlocksEJS = entityIndex.getNoOfCommonBlocks(blockIndex, comparison);
                if (commonBlocksEJS < 0) {
                    return commonBlocksEJS;
                }

                double probability = commonBlocksEJS / (entityIndex.getNoOfEntityBlocks((int) comparison.getEntityId1(), 0) + entityIndex.getNoOfEntityBlocks((int) comparison.getEntityId2(), comparison.isCleanCleanER()?1:0) - commonBlocksEJS);
                return probability * Math.log10(validComparisons / comparisonsPerEntity[(int) comparison.getEntityId1()]) * Math.log10(validComparisons / comparisonsPerEntity[comparison.isCleanCleanER()? (int) comparison.getEntityId2()+entityIndex.getDatasetLimit():(int) comparison.getEntityId2()]);
        }

        return -1;
    }
    
    public void resetEntityIndex() {
        entityIndex = null;
    }
}