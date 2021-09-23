

package gr.athenarc.imsi.visualfacts.queryER.DataStructures;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class GroundTruthEntityIndex {

    private int datasetLimit;
    private int noOfEntities;
    private long[][] entityBlocks;
    private Set<IdDuplicates> duplicates;

    public GroundTruthEntityIndex(List<AbstractBlock> blocks, Set<IdDuplicates> matches) {
        if (blocks.isEmpty()) {
            System.err.println("Entity index received an empty block collection as input!");
            return;
        }

        if (blocks.get(0) instanceof DecomposedBlock) {
            System.err.println("The entity index is incompatible with a set of decomposed blocks!");
            System.err.println("Its functionalities can be carried out with same efficiency through a linear search of all comparisons!");
            return;
        }
        
        duplicates = matches;
        enumerateBlocks(blocks);
        setNoOfEntities(blocks);
        indexEntities(blocks);
//        for(long[] e : entityBlocks) {
//        	for(long ee : e) {
//        		System.out.print(ee  + ' ');
//        	}
//        	System.out.println();
//        }
//        System.out.println(entityBlocks);

    }

    private void enumerateBlocks(List<AbstractBlock> blocks) {
        int blockIndex = 0;
        for (AbstractBlock block : blocks) {
            block.setBlockIndex(blockIndex++);
        }
    }

    public List<Long> getCommonBlockIndices(int blockIndex, Comparison comparison) {
    	long[] blocks1 = entityBlocks[(int) comparison.getEntityId1()];
        long[] blocks2 = entityBlocks[(int) comparison.getEntityId2() + datasetLimit];

        boolean firstCommonIndex = false;
        int noOfBlocks1 = blocks1.length;
        int noOfBlocks2 = blocks2.length;
        final List<Long> indices = new ArrayList<Long>();
        for (int i = 0; i < noOfBlocks1; i++) {
            for (int j = 0; j < noOfBlocks2; j++) {
                if (blocks2[j] < blocks1[i]) {
                    continue;
                }

                if (blocks1[i] < blocks2[j]) {
                    break;
                }

                if (blocks1[i] == blocks2[j]) {
                    if (!firstCommonIndex) {
                        firstCommonIndex = true;
                        if (blocks1[i] != blockIndex) {
                            return null;
                        }
                    }
                    indices.add(blocks1[i]);
                }
            }
        }
        return indices;
    }

    public int getDatasetLimit() {
        return datasetLimit;
    }

    public long[] getEntityBlocks(long l, int useDLimit) {
        l += useDLimit * datasetLimit;
        if (noOfEntities <= l) {
            return null;
        }
        return (long[]) entityBlocks[(int) l];
    }

    public int getNoOfCommonBlocks(int blockIndex, Comparison comparison) {
        long[] blocks1 = entityBlocks[(int) comparison.getEntityId1()];
        long[] blocks2 = entityBlocks[(int) comparison.getEntityId2() + datasetLimit];

        boolean firstCommonIndex = false;
        int commonBlocks = 0;
        int noOfBlocks1 = blocks1.length;
        int noOfBlocks2 = blocks2.length;
        for (int i = 0; i < noOfBlocks1; i++) {
            for (int j = 0; j < noOfBlocks2; j++) {
                if (blocks2[j] < blocks1[i]) {
                    continue;
                }

                if (blocks1[i] < blocks2[j]) {
                    break;
                }

                if (blocks1[i] == blocks2[j]) {
                    commonBlocks++;
                    if (!firstCommonIndex) {
                        firstCommonIndex = true;
                        if (blocks1[i] != blockIndex) {
                            return -1;
                        }
                    }
                }
            }
        }

        return commonBlocks;
    }

    public int getNoOfEntities() {
        return noOfEntities;
    }

    public int getNoOfEntityBlocks(int entityId, int useDLimit) {
        entityId += useDLimit * datasetLimit;
        if (entityBlocks[entityId] == null) {
            return -1;
        }

        return entityBlocks[entityId].length;
    }

    public List<Long> getTotalCommonIndices(Comparison comparison) {
        final List<Long> indices = new ArrayList<Long>();

        long[] blocks1 = entityBlocks[(int) comparison.getEntityId1()];
        long[] blocks2 = entityBlocks[(int) comparison.getEntityId2() + datasetLimit];
        if (blocks1 == null || blocks2 == null) {
            return indices;
        }

        int noOfBlocks1 = blocks1.length;
        int noOfBlocks2 = blocks2.length;
        for (int i = 0; i < noOfBlocks1; i++) {
            for (int j = 0; j < noOfBlocks2; j++) {
                if (blocks2[j] < blocks1[i]) {
                    continue;
                }

                if (blocks1[i] < blocks2[j]) {
                    break;
                }

                if (blocks1[i] == blocks2[j]) {
                    indices.add(blocks1[i]);
                }
            }
        }

        return indices;
    }

    public int getTotalNoOfCommonBlocks(Comparison comparison) {
        long[] blocks1 = entityBlocks[(int) comparison.getEntityId1()];
        long[] blocks2 = entityBlocks[(int) comparison.getEntityId2() + datasetLimit];
        if (blocks1 == null || blocks2 == null) {
            return 0;
        }

        int commonBlocks = 0;
        int noOfBlocks1 = blocks1.length;
        int noOfBlocks2 = blocks2.length;
        for (int i = 0; i < noOfBlocks1; i++) {
            for (int j = 0; j < noOfBlocks2; j++) {
                if (blocks2[j] < blocks1[i]) {
                    continue;
                }

                if (blocks1[i] < blocks2[j]) {
                    break;
                }

                if (blocks1[i] == blocks2[j]) {
                    commonBlocks++;
                }
            }
        }

        return commonBlocks;
    }

    private void indexBilateralEntities(List<AbstractBlock> blocks) {
        //find matching entities
        Set<Long> matchingEntities = new HashSet<Long>();
        for (IdDuplicates pair : duplicates) {
            matchingEntities.add((long) pair.getEntityId1());
            matchingEntities.add((long) pair.getEntityId2() + datasetLimit);
        }
        
        //count blocks per matching entity
        int[] counters = new int[noOfEntities];
        for (AbstractBlock block : blocks) {
            BilateralBlock bilBlock = (BilateralBlock) block;
            for (long id1 : bilBlock.getIndex1Entities()) {
                if (matchingEntities.contains(id1)) {
                    counters[(int) id1]++;
                }
            }
            
            for (long id2 : bilBlock.getIndex2Entities()) {
                long entityId = (datasetLimit+id2);
                if (matchingEntities.contains(entityId)) {
                    counters[(int)entityId]++;
                }
            }
        }
        
        //initialize inverted index
        entityBlocks = new long[noOfEntities][];
        for (int i = 0; i < noOfEntities; i++) {
            entityBlocks[i] = new long[counters[i]];
            counters[i] = 0;
        }
        
        //build inverted index
        for (AbstractBlock block : blocks) {
            BilateralBlock bilBlock = (BilateralBlock) block;
            for (long id1 : bilBlock.getIndex1Entities()) {
                if (matchingEntities.contains(id1)) {
                    entityBlocks[(int) id1][counters[(int) id1]] = block.getBlockIndex();
                    counters[(int) id1]++;
                }
            }
            
            for (long id2 : bilBlock.getIndex2Entities()) {
                long entityId = (int) (datasetLimit+id2);
                if (matchingEntities.contains(entityId)) {
                    entityBlocks[(int)entityId][counters[(int)entityId]] = block.getBlockIndex();
                    counters[(int)entityId]++;
                }
            }
        }
    }

    private void indexEntities(List<AbstractBlock> blocks) {
        if (blocks.get(0) instanceof BilateralBlock) {
            indexBilateralEntities(blocks);
        } else if (blocks.get(0) instanceof UnilateralBlock) {
            indexUnilateralEntities(blocks);
        }
    }

    private void indexUnilateralEntities(List<AbstractBlock> blocks) {
        //find matching entities

        Set<Long> matchingEntities = new HashSet<Long>();
        for (IdDuplicates pair : duplicates) {
            matchingEntities.add( pair.getEntityId1());
            matchingEntities.add( pair.getEntityId2());
        }

        //count blocks per matching entity
        int[] counters = new int[noOfEntities];
        for (AbstractBlock block : blocks) {
            UnilateralBlock uniBlock = (UnilateralBlock) block;
            for (long id : uniBlock.getEntities()) {
                if (matchingEntities.contains(id)) {
                    counters[(int) id]++;
                }
            }
        }
        
        //initialize inverted index
        entityBlocks = new long[noOfEntities][];
        for (int i = 0; i < noOfEntities; i++) {
            entityBlocks[i] = new long[counters[i]];
            counters[i] = 0;
        }
        
        //build inverted index
        for (AbstractBlock block : blocks) {
            UnilateralBlock uniBlock = (UnilateralBlock) block;
            for (long id : uniBlock.getEntities()) {
                if (matchingEntities.contains(id)) {
                	//System.out.println(id + ", " + counters[(int) id] + " = " + block.getBlockIndex());
                    entityBlocks[(int) id][counters[(int) id]] = block.getBlockIndex();
                    counters[(int) id]++;
                }
            }
        }
    }

    public boolean isRepeated(int blockIndex, Comparison comparison) {
        long[] blocks1 = entityBlocks[(int) comparison.getEntityId1()];
        long[] blocks2 = entityBlocks[(int) comparison.getEntityId2() + datasetLimit];

        int noOfBlocks1 = blocks1.length;
        int noOfBlocks2 = blocks2.length;
        for (int i = 0; i < noOfBlocks1; i++) {
            for (int j = 0; j < noOfBlocks2; j++) {
                if (blocks2[j] < blocks1[i]) {
                    continue;
                }

                if (blocks1[i] < blocks2[j]) {
                    break;
                }

                if (blocks1[i] == blocks2[j]) {
                    return blocks1[i] != blockIndex;
                }
            }
        }

        System.err.println("Error!!!!");
        return false;
    }

    private void setNoOfEntities(List<AbstractBlock> blocks) {
        if (blocks.get(0) instanceof UnilateralBlock) {
            setNoOfUnilateralEntities(blocks);
        } else if (blocks.get(0) instanceof BilateralBlock) {
            setNoOfBilateralEntities(blocks);
        }
    }

    private void setNoOfBilateralEntities(List<AbstractBlock> blocks) {
        noOfEntities = Integer.MIN_VALUE;
        datasetLimit = Integer.MIN_VALUE;
        for (AbstractBlock block : blocks) {
            BilateralBlock bilBlock = (BilateralBlock) block;
            for (long id1 : bilBlock.getIndex1Entities()) {
                if (noOfEntities < id1 + 1) {
                    noOfEntities = (int) (id1 + 1);
                }
            }

            for (long id2 : bilBlock.getIndex2Entities()) {
                if (datasetLimit < id2 + 1) {
                    datasetLimit = (int) (id2 + 1);
                }
            }
        }

        int temp = noOfEntities;
        noOfEntities += datasetLimit;
        datasetLimit = temp;
    }

    private void setNoOfUnilateralEntities(List<AbstractBlock> blocks) {
        noOfEntities = Integer.MIN_VALUE;
        datasetLimit = 0;
        for (AbstractBlock block : blocks) {
            UnilateralBlock bilBlock = (UnilateralBlock) block;
            for (long id : bilBlock.getEntities()) {
                if (noOfEntities < id + 1) {
                    noOfEntities = (int) (id + 1);
                }
            }
        }
    }
}
