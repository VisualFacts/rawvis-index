package gr.athenarc.imsi.visualfacts;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.Random;

public class SamplingNodePointsIterator extends AbstractNodePointIterator {
    private final BitSet selectedSamples; // Holds only eligible sampled points
    private int currentIndex; // Tracks progress over selectedSamples

    private static final Logger LOG = LogManager.getLogger(SamplingNodePointsIterator.class);

    public SamplingNodePointsIterator(QueryNode queryNode, double samplingRate) {
        this.queryNode = queryNode;
        int intersectionCount = queryNode.getIntersectionCount();
        int targetSampleCount = Math.max(2, (int) Math.ceil(samplingRate * intersectionCount));

        // Compute remaining samples needed
        int alreadySampled = queryNode.getSampledTracker().cardinality();
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
     * Implements reservoir sampling to select exactly `remainingSamplesNeeded`
     * points
     * from the set of query-intersecting and unsampled points.
     */
    private BitSet selectRandomBitsReservoir(int remainingSamplesNeeded) {
        BitSet reservoir = new BitSet();

        // Compute eligible points: within query AND not previously sampled
        BitSet eligiblePoints = (BitSet) queryNode.getQueryPointsBitSet().clone();
        eligiblePoints.andNot(queryNode.getSampledTracker()); // Remove previously sampled points

        Random random = new Random();
        int reservoirSize = 0; // Tracks selected samples count

        for (int index = eligiblePoints.nextSetBit(0),
                processedCount = 0; index >= 0; index = eligiblePoints.nextSetBit(index + 1), processedCount++) {

            if (reservoirSize < remainingSamplesNeeded) {
                // Fill reservoir with query-intersecting, unsampled points
                reservoir.set(index);
                reservoirSize++;
            } else {
                // Replace existing points with decreasing probability
                int r = random.nextInt(processedCount + 1);
                if (r < remainingSamplesNeeded) {
                    // Replace an existing point in the reservoir
                    // Pick a random existing sample in the reservoir to remove,
                    int toRemove = getRandomSetBit(reservoir, random);
                    reservoir.clear(toRemove);
                    reservoir.set(index);
                }
            }
        }

        return reservoir;
    }

        /**
     * Returns the index of a randomly chosen set bit from the given reservoir BitSet.
     * This ensures uniform selection among the currently set bits.
     */
    private int getRandomSetBit(BitSet reservoir, Random random) {
        int size = reservoir.cardinality();
        // Pick which set bit we want to remove
        int target = random.nextInt(size);

        // Iterate through the set bits to find the target-th one
        int current = reservoir.nextSetBit(0);
        for (int count = 0; count < target; count++) {
            current = reservoir.nextSetBit(current + 1);
        }
        return current;
    }

    @Override
    protected Point getNext() {
        TreeNode node = queryNode.getNode();

        // Directly iterate over selectedSamples instead of scanning all points
        if (currentIndex >= 0) {
            Point point = node.getPoints().get(currentIndex);
            queryNode.getSampledTracker().set(currentIndex); // Mark as sampled

            // Move to next selected sample
            currentIndex = selectedSamples.nextSetBit(currentIndex + 1);
            return point;
        }

        return null; // No more samples
    }

    public QueryNode getQueryNode() {
        return queryNode;
    }
}
