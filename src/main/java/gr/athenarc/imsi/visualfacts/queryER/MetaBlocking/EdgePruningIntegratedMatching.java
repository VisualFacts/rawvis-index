package gr.athenarc.imsi.visualfacts.queryER.MetaBlocking;

import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.Comparison;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.EntityProfile;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.ComparisonIterator;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.MetaBlockingConfiguration.WeightingScheme;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.ProfileComparison;
import java.util.List;

/**
 *
 * @author gap2
 */
public class EdgePruningIntegratedMatching extends EdgePruning {

    private final EntityProfile[] profiles1;
    private final EntityProfile[] profiles2;

    public EdgePruningIntegratedMatching(List<EntityProfile> pr1,
            List<EntityProfile> pr2, WeightingScheme scheme) {
        super("Edge Pruning with integrated matching", scheme);
        profiles1 = pr1.toArray(new EntityProfile[pr1.size()]);
        if (pr2 == null) {
            profiles2 = null;
        } else {
            profiles2 = pr2.toArray(new EntityProfile[pr2.size()]);
        }
    }

    @Override
    protected void filterComparisons(List<AbstractBlock> blocks) {
        boolean cleanCleanER = profiles2 != null;
        for (AbstractBlock block : blocks) {
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                double weight = getWeight(block.getBlockIndex(), comparison);
                if (weight < averageWeight) {
                    continue;
                }
                if (cleanCleanER) {
                    ProfileComparison.getJaccardSimilarity(profiles1[(int) comparison.getEntityId1()].getAttributes(),
                            profiles2[(int) comparison.getEntityId2()].getAttributes());
                } else {
                    ProfileComparison.getJaccardSimilarity(profiles1[(int) comparison.getEntityId1()].getAttributes(),
                            profiles1[(int) comparison.getEntityId2()].getAttributes());
                }
            }
        }

    }
}
