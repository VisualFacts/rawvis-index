package gr.athenarc.imsi.visualfacts.queryER.MetaBlocking;


import java.util.List;

import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.MetaBlockingConfiguration.WeightingScheme;


public class EfficientEdgePruning extends EdgePruning {

    public EfficientEdgePruning() {
        super("Efficient Edge Pruning", WeightingScheme.CBS);
        averageWeight = 2.0;
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
    	getStatistics(blocks);
    	//initializeEntityIndex(blocks);
        filterComparisons(blocks);
    }
}
