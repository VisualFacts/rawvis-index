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

package gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.BlockRefinement;


import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.BilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.UnilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.AbstractEfficiencyMethod;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SizeBasedBlockPurging extends AbstractEfficiencyMethod {
    
    private final static double PURGING_FACTOR = 0.005;
    
    public SizeBasedBlockPurging() {
        super("(Size-based) Block Purging");
    }
    
    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        //only applicable to bilateral block collections
        if (blocks.get(0) instanceof UnilateralBlock) {
            return;
        }
        
        int maxInnerBlockSize = getMaxInnerBlockSize(blocks);
        
        double totalComparisons = 0;
        final List<AbstractBlock> purgedBlocks = new ArrayList<AbstractBlock>();
        for (AbstractBlock block : blocks) {
            final BilateralBlock bilBlock = (BilateralBlock) block;
            if (maxInnerBlockSize < Math.min(bilBlock.getIndex1Entities().length, bilBlock.getIndex2Entities().length)) {
                purgedBlocks.add(block);
            } else {
                totalComparisons += block.getNoOfComparisons();
            }
        }
        blocks.removeAll(purgedBlocks);
        
        System.out.println("Purged blocks\t:\t" + purgedBlocks.size());
        System.out.println("Retained blocks\t:\t" + blocks.size());
        System.out.println("Retained comparisons\t:\t" + totalComparisons);
    }
    
    private int getMaxInnerBlockSize(List<AbstractBlock> blocks) {
        final Set<Integer> d1Entities = new HashSet<Integer>();
        final Set<Integer> d2Entities = new HashSet<Integer>();
        for (AbstractBlock block : blocks) {
            final BilateralBlock bilBlock = (BilateralBlock) block;
            for (long id1 : bilBlock.getIndex1Entities()) {
                d1Entities.add((int) id1);
            }
            
            for (long id2 : bilBlock.getIndex2Entities()) {
                d2Entities.add((int) id2);
            }
        }
        
        return (int) Math.round(Math.min(d1Entities.size(), d2Entities.size())*PURGING_FACTOR);
    }
}