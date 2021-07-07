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


import gr.athenarc.imsi.visualfacts.queryER.Comparators.BlockCardinalityComparator;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.BilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.UnilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.AbstractEfficiencyMethod;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.Converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BlockFiltering extends AbstractEfficiencyMethod {

    protected final double ratio;

    protected int entitiesD1;
    protected int entitiesD2;
    protected long[] counterD1;
    protected long[] counterD2;
    protected long[] limitsD1;
    protected long[] limitsD2;

    public BlockFiltering(double r) {
        this(r, "Block Filtering");
    }
    
    public BlockFiltering(double r, String description) {
        super(description);
        ratio = r;
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        countEntities(blocks);
        sortBlocks(blocks);
        getLimits(blocks);
        initializeCounters();
        restructureBlocks(blocks);
    }

    protected void countEntities(List<AbstractBlock> blocks) {
        entitiesD1 = Integer.MIN_VALUE;
        entitiesD2 = Integer.MIN_VALUE;
        if (blocks.get(0) instanceof BilateralBlock) {
            for (AbstractBlock block : blocks) {
                BilateralBlock bilBlock = (BilateralBlock) block;
                for (long id1 : bilBlock.getIndex1Entities()) {
                    if (entitiesD1 < id1 + 1) {
                        entitiesD1 = (int) (id1 + 1);
                    }
                }
                for (long id2 : bilBlock.getIndex2Entities()) {
                    if (entitiesD2 < id2 + 1) {
                        entitiesD2 = (int) (id2 + 1);
                    }
                }
            }
        } else if (blocks.get(0) instanceof UnilateralBlock) {
            for (AbstractBlock block : blocks) {
                UnilateralBlock uniBlock = (UnilateralBlock) block;
                for (long id : uniBlock.getEntities()) {
                    if (entitiesD1 < id + 1) {
                        entitiesD1 = (int) (id + 1);
                    }
                }
            }
        }
    }

    protected void getBilateralLimits(List<AbstractBlock> blocks) {
        limitsD1 = new long[entitiesD1];
        limitsD2 = new long[entitiesD2];
        for (AbstractBlock block : blocks) {
            BilateralBlock bilBlock = (BilateralBlock) block;
            for (long id1 : bilBlock.getIndex1Entities()) {
                limitsD1[(int) id1]++;
            }
            for (long id2 : bilBlock.getIndex2Entities()) {
                limitsD2[(int) id2]++;
            }
        }

        for (int i = 0; i < limitsD1.length; i++) {
            limitsD1[i] = (int) Math.round(ratio * limitsD1[i]);
        }
        for (int i = 0; i < limitsD2.length; i++) {
            limitsD2[i] = (int) Math.round(ratio * limitsD2[i]);
        }
    }

    protected void getLimits(List<AbstractBlock> blocks) {
        if (blocks.get(0) instanceof BilateralBlock) {
            getBilateralLimits(blocks);
        } else if (blocks.get(0) instanceof UnilateralBlock) {
            getUnilateralLimits(blocks);
        }
    }

    protected void getUnilateralLimits(List<AbstractBlock> blocks) {
        limitsD1 = new long[entitiesD1];
        limitsD2 = null;
        for (AbstractBlock block : blocks) {
            UnilateralBlock uniBlock = (UnilateralBlock) block;
            for (long id : uniBlock.getEntities()) {
                limitsD1[(int) id]++;
            }
        }

        for (int i = 0; i < limitsD1.length; i++) {
            limitsD1[i] = (int) Math.round(ratio * limitsD1[i]);
        }
    }

    protected void initializeCounters() {
        counterD1 = new long[entitiesD1];
        counterD2 = null;
        if (0 < entitiesD2) {
            counterD2 = new long[entitiesD2];
        }
    }

    protected void restructureBilateraBlocks(List<AbstractBlock> blocks) {
        final List<AbstractBlock> newBlocks = new ArrayList<AbstractBlock>();
        for (AbstractBlock block : blocks) {
            BilateralBlock oldBlock = (BilateralBlock) block;
            final List<Long> retainedEntitiesD1 = new ArrayList<Long>();
            for (long entityId : oldBlock.getIndex1Entities()) {
                if (counterD1[(int) entityId] < limitsD1[(int) entityId]) {
                    retainedEntitiesD1.add(entityId);
                }
            }

            final List<Long> retainedEntitiesD2 = new ArrayList<Long>();
            for (long entityId : oldBlock.getIndex2Entities()) {
                if (counterD2[(int) entityId] < limitsD2[(int) entityId]) {
                    retainedEntitiesD2.add(entityId);
                }
            }

            if (!retainedEntitiesD1.isEmpty() && !retainedEntitiesD2.isEmpty()) {
            	long[] blockEntitiesD1 = Converter.convertCollectionToArray(retainedEntitiesD1);
                for (long entityId : blockEntitiesD1) {
                    counterD1[(int) entityId]++;
                }
                long[] blockEntitiesD2 = Converter.convertCollectionToArray(retainedEntitiesD2);
                for (long entityId : blockEntitiesD2) {
                    counterD2[(int) entityId]++;
                }
                newBlocks.add(new BilateralBlock(blockEntitiesD1, blockEntitiesD2));
            }
        }
        blocks.clear();
        blocks.addAll(newBlocks);
    }

    protected void restructureBlocks(List<AbstractBlock> blocks) {
        if (blocks.get(0) instanceof BilateralBlock) {
            restructureBilateraBlocks(blocks);
        } else if (blocks.get(0) instanceof UnilateralBlock) {
            restructureUnilateraBlocks(blocks);
        }
    }

    protected void restructureUnilateraBlocks(List<AbstractBlock> blocks) {
        final List<AbstractBlock> newBlocks = new ArrayList<AbstractBlock>();
        for (AbstractBlock block : blocks) {
            UnilateralBlock oldBlock = (UnilateralBlock) block;
            final List<Long> retainedEntities = new ArrayList<Long>();
            for (long entityId : oldBlock.getEntities()) {
                if (counterD1[(int) entityId] < limitsD1[(int) entityId]) {
                    retainedEntities.add(entityId);
                }
            }

            if (1 < retainedEntities.size()) {
                long[] blockEntities = Converter.convertCollectionToArray(retainedEntities);
                for (long entityId : blockEntities) {
                    counterD1[(int) entityId]++;
                }
                newBlocks.add(new UnilateralBlock(blockEntities));
            }
        }
        blocks.clear();
        blocks.addAll(newBlocks);
    }

    protected void sortBlocks(List<AbstractBlock> blocks) {
        Collections.sort(blocks, new BlockCardinalityComparator());
    }
}
