package gr.athenarc.imsi.visualfacts.queryER.MetaBlocking;

import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.BilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.Comparison;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.AbstractMetablocking;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.ComparisonIterator;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.MetaBlockingConfiguration.WeightingScheme;

import java.util.ArrayList;
import java.util.List;

public class EdgePruning extends AbstractMetablocking {

    protected double averageWeight;

    public EdgePruning(WeightingScheme scheme) {
        super("Edge Pruning", scheme);
    }

    protected EdgePruning(String description, WeightingScheme scheme) {
        super(description, scheme);
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        getStatistics(blocks);
        setAverageWeight(blocks);
        filterComparisons(blocks);
    }

    protected void filterComparisons(List<AbstractBlock> blocks) {
        if (blocks.isEmpty()) return;
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

    private void setAverageWeight(List<AbstractBlock> blocks) {
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
            }
        }
        averageWeight /= validComparisons;
        System.out.println("Average weight\t:\t" + averageWeight);
    }
}
