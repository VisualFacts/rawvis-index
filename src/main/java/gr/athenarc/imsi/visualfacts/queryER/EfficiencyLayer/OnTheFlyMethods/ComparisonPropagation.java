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

package gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.OnTheFlyMethods;


import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.Comparison;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.EntityIndex;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.ComparisonIterator;

import java.util.List;


public class ComparisonPropagation {
    
    protected EntityIndex entityIndex;
    protected double uniqueComparisons;
    
    public ComparisonPropagation() {
        uniqueComparisons = 0;
    }
    
    public void applyProcessing(List<AbstractBlock> blocks) {
        entityIndex = new EntityIndex(blocks);
        for (AbstractBlock block : blocks) {
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                if (!entityIndex.isRepeated(block.getBlockIndex(), comparison)) {
                    uniqueComparisons++;
                }
            }
        }
        System.out.println("Unique comparisons\t:\t" + uniqueComparisons);
    }
    
    public double getComparisons() {
        return uniqueComparisons;
    }
}