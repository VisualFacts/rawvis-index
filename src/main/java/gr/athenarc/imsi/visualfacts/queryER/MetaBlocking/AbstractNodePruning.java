/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */

package gr.athenarc.imsi.visualfacts.queryER.MetaBlocking;


import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.BilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.Comparison;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.UnilateralBlock;
import java.util.HashSet;
import java.util.List;
import java.util.Set;



public abstract class AbstractNodePruning extends AbstractMetablocking {
    
    protected boolean cleanCleanER;
    protected AbstractBlock[] blocksArray;
    
    public AbstractNodePruning(String description, WeightingScheme scheme) {
        super(description, scheme);
    }
    
    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        getStatistics(blocks);
        
        cleanCleanER = blocks.get(0) instanceof BilateralBlock;
        blocksArray = blocks.toArray(new AbstractBlock[blocks.size()]);
        blocks.clear();
        
        processPartition(0, entityIndex.getDatasetLimit(), blocks);
        processPartition(entityIndex.getDatasetLimit(), entityIndex.getNoOfEntities(), blocks);
    }
    
    protected Integer[] getAdjacentEntities(int entityId) { // continuous entity id
        long[] associatedBlocks = entityIndex.getEntityBlocks(entityId, 0);
        if (associatedBlocks.length == 0) { // singleton entity
            return null;
        }
        
        Set<Integer> adjacentEntities = new HashSet<Integer>();
        if (!cleanCleanER) {
            for (long blockIndex : associatedBlocks) {
                UnilateralBlock block = (UnilateralBlock) blocksArray[(int) blockIndex];
                for (long neighborId : block.getEntities()) {
                    adjacentEntities.add((int) neighborId);
                }
            }
            adjacentEntities.remove(entityId);
        } else {
            boolean firstPartition = entityId < entityIndex.getDatasetLimit();
            for (long blockIndex : associatedBlocks) {
                BilateralBlock block = (BilateralBlock) blocksArray[(int) blockIndex];
                if (firstPartition) {
                    for (long neighborId : block.getIndex2Entities()) {
                        adjacentEntities.add((int) neighborId);
                    }
                } else {
                    for (long neighborId : block.getIndex1Entities()) {
                        adjacentEntities.add((int) neighborId);
                    }
                }
            }
        }
        
        return adjacentEntities.toArray(new Integer[adjacentEntities.size()]);
    }
    
    protected Comparison getComparison(int entityId1, int entityId2) {
        if (cleanCleanER) {
            if (entityIndex.getDatasetLimit() <= entityId1) {
                //entity 1 belongs to the second/right partition and its id should be normalized
                return new Comparison(cleanCleanER, entityId2, entityId1-entityIndex.getDatasetLimit());
            } else {
                return new Comparison(cleanCleanER, entityId1, entityId2);
            }
        }
        
        if (entityId1 < entityId2) {
            return new Comparison(cleanCleanER, entityId1, entityId2);
        }
        
        return new Comparison(cleanCleanER, entityId2, entityId1);
    }
    
    protected abstract void processPartition(int firstId, int lastId, List<AbstractBlock> blocks);
}