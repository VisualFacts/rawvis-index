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

//import DataStructures.AbstractBlock;
//import DataStructures.BilateralBlock;
//import DataStructures.Comparison;
//import Utilities.ComparisonIterator;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.BilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.Comparison;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.ComparisonIterator;

import java.util.ArrayList;
import java.util.List;

public class WeightedEdgePruning extends AbstractMetablocking {

    protected double averageWeight;

    public WeightedEdgePruning(WeightingScheme scheme) {
        this("Weighted Edge Pruning", scheme);
    }
    
    protected WeightedEdgePruning(String description, WeightingScheme scheme) {
        super(description, scheme);
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        getStatistics(blocks);
        setAverageWeight(blocks);
        filterComparisons(blocks);
    }

    protected void filterComparisons(List<AbstractBlock> blocks) {
        boolean cleanCleanER = blocks.get(0) instanceof BilateralBlock;
        final List<AbstractBlock> newBlocks = new ArrayList<AbstractBlock>();
        for (AbstractBlock block : blocks) {
            final List<Long> entities1 = new ArrayList<Long>();
            final List<Long> entities2 = new ArrayList<Long>();

            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                double weight = getWeight(block.getBlockIndex(), comparison);
                if (weight < averageWeight) {
                    continue;
                }

                entities1.add(comparison.getEntityId1());
                entities2.add(comparison.getEntityId2());
            }
            newBlocks.add(getDecomposedBlock(cleanCleanER, entities1, entities2));
        }
        blocks.clear();
        blocks.addAll(newBlocks);
    }

    protected void setAverageWeight(List<AbstractBlock> blocks) {
        double totalComparisons = 0;
        averageWeight = 0;
        for (AbstractBlock block : blocks) {
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                double weight = getWeight(block.getBlockIndex(), comparison);
                if (weight < 0) {
                    continue;
                }

                averageWeight += weight;
                totalComparisons++;
            }
        }
        averageWeight /= totalComparisons;
    }
}
