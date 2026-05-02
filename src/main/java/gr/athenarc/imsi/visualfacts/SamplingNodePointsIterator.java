package gr.athenarc.imsi.visualfacts;

import java.util.BitSet;
import java.util.Random;

public class SamplingNodePointsIterator extends AbstractNodePointIterator {
    private final BitSet selectedSamples; // Holds only eligible sampled points
    private int currentIndex; // Tracks progress over selectedSamples

    public SamplingNodePointsIterator(QueryNode queryNode, double samplingRate) {
        this(queryNode, Math.max(2, (int) Math.ceil(samplingRate * queryNode.getIntersectionCount())));
    }

    /**
     * Absolute-target constructor used by the stratified Neyman+FPC
     * allocator: requests exactly {@code targetSampleCount} total samples
     * from this node (cumulative, including any already-sampled points from
     * prior rounds).
     */
    public SamplingNodePointsIterator(QueryNode queryNode, int targetSampleCount) {
        this.queryNode = queryNode;
        int intersectionCount = queryNode.getIntersectionCount();
        if (targetSampleCount > intersectionCount) targetSampleCount = intersectionCount;
        if (targetSampleCount < 0) targetSampleCount = 0;

        // Compute remaining samples needed
        int alreadySampled = queryNode.getSampledPointCount();
        int remainingSamplesNeeded = Math.max(targetSampleCount - alreadySampled, 0); // Ensure non-negative

        // If no additional samples are needed, exit early
        if (remainingSamplesNeeded == 0) {
            this.selectedSamples = new BitSet(); // Empty bitset
            this.currentIndex = -1; // Nothing to iterate
            return;
        }

        // Precompute selected samples
        this.selectedSamples = selectRandomBitsReservoir(remainingSamplesNeeded);
        this.currentIndex = selectedSamples.nextSetBit(0); // Start from first selected sample
    }

    /**
     * Selects exactly {@code remainingSamplesNeeded} random points from the
     * eligible (query-intersecting and not yet sampled) population.
     * Uses O(k) rejection sampling for fully-contained tiles, or
     * O(eligible + k) enumerate-then-Fisher-Yates for partial tiles.
     */
    private BitSet selectRandomBitsReservoir(int remainingSamplesNeeded) {
        // Fast path: fully-contained tile; eligible indices are [0, tileSize) \ sampledTracker.
        // Rejection sampling generates k random ints in [0, tileSize), rejecting collisions.
        // Expected cost: O(k / (1-f)) where f = sampled fraction. Falls through if f >= 50%.
        if (queryNode.isFullyContained()) {
            int tileSize = queryNode.getTile().getSize();
            BitSet sampledTracker = queryNode.getSampledTracker();
            // Outlier-aware AQP: outliers are part of the population but excluded
            // from the sampling pool.  When present, they are added to the "blocked"
            // set passed to the rejection sampler so they are never emitted.
            BitSet outliers = queryNode.getTile().getOutlierBitSet();
            BitSet blocked = (BitSet) sampledTracker.clone();
            if (outliers != null) {
                blocked.or(outliers);
            }
            int blockedCount = blocked.cardinality();
            if (blockedCount < tileSize / 2) {
                int eligibleCount = tileSize - blockedCount;
                int k = Math.min(remainingSamplesNeeded, eligibleCount);
                return selectRandomBitsRejection(k, tileSize, sampledTracker, outliers);
            }
        }

        // General path: sparse eligible population; enumerate then partial Fisher-Yates
        BitSet eligiblePoints = (BitSet) queryNode.getQueryPointsBitSet().clone();
        eligiblePoints.andNot(queryNode.getSampledTracker());

        int eligibleCount = eligiblePoints.cardinality();

        // Collect all eligible bit positions into an array (single pass)
        int[] indices = new int[eligibleCount];
        int pos = 0;
        for (int index = eligiblePoints.nextSetBit(0); index >= 0;
                index = eligiblePoints.nextSetBit(index + 1)) {
            indices[pos++] = index;
        }

        int k = Math.min(remainingSamplesNeeded, eligibleCount);

        // Partial Fisher-Yates: shuffle only the first k positions in O(k)
        Random random = new Random();
        for (int i = 0; i < k; i++) {
            int j = i + random.nextInt(eligibleCount - i); // uniform in [i, eligibleCount)
            int tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
        }

        // Build result BitSet from the first k shuffled positions
        BitSet result = new BitSet();
        for (int i = 0; i < k; i++) {
            result.set(indices[i]);
        }
        return result;
    }

    /**
     * Selects k random indices from [0, tileSize) avoiding sampledTracker (and
     * optionally outlierBitSet) via rejection.  Each eligible index has equal
     * probability k/eligible of being selected (SRSWOR).
     */
    private BitSet selectRandomBitsRejection(int k, int tileSize, BitSet sampledTracker, BitSet outliers) {
        Random random = new Random();
        BitSet result = new BitSet();
        int selected = 0;
        while (selected < k) {
            int idx = random.nextInt(tileSize);
            if (sampledTracker.get(idx) || result.get(idx)) continue;
            // Outlier-aware AQP: skip outlier positions so they are excluded
            // from the sampling pool (they contribute exact closed-form sums
            // separately in the CI computation).
            if (outliers != null && outliers.get(idx)) continue;
            result.set(idx);
            selected++;
        }
        return result;
    }

    @Override
    protected boolean advanceInternal() {
        if (currentIndex < 0) {
            return false;
        }
        // Mark current as sampled
        queryNode.getSampledTracker().set(currentIndex);
        return true;
    }

    @Override
    protected long peekOffset() {
        long offset = queryNode.getTile().getOffset(currentIndex);
        // Move to next selected sample for the next advance call
        currentIndex = selectedSamples.nextSetBit(currentIndex + 1);
        return offset;
    }

    public QueryNode getQueryNode() {
        return queryNode;
    }
}
