package gr.athenarc.imsi.visualfacts;

import static org.junit.jupiter.api.Assertions.*;

import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.query.Query;

class OutlierAwareSplitTest {

    @Test
    void splitPropagatesOutlierMetadataToChildren() {
        QuadTreeTile tile = tileWithFourQuadrantPoints();
        BitSet parentOutliers = new BitSet(4);
        parentOutliers.set(1);
        parentOutliers.set(3);
        tile.setOutlierData(parentOutliers, new int[] { -1, 7, -1, 11 });

        tile.split();

        assertChildOutlier(tile, 200L, 7);
        assertChildOutlier(tile, 400L, 11);
        assertChildHasNoOutliers(tile, 100L);
        assertChildHasNoOutliers(tile, 300L);
    }

    @Test
    void cachedFullyContainedSamplingNodeStillTrimsOutliers() {
        QuadTreeTile tile = tileWithFourQuadrantPoints();
        BitSet sampled = new BitSet(4);
        sampled.set(0);
        tile.setSampledTracker(sampled);

        BitSet parentOutliers = new BitSet(4);
        parentOutliers.set(2);
        tile.setOutlierData(parentOutliers, new int[] { -1, -1, 5, -1 });

        Schema schema = new Schema("unused.csv", ',', 0, 1, List.of(2),
                rectangle(0, 4, 0, 4), 4, List.of());
        Query query = new Query(rectangle(0, 4, 0, 4), List.of(2));

        QueryNode node = new QueryNode(tile, null, query, schema);

        assertEquals(3, node.getIntersectionCount());
        assertNotNull(node.getInQueryOutliers());
        assertTrue(node.getInQueryOutliers().get(2));
        assertFalse(node.getQueryPointsBitSet().get(2));
    }

    @Test
    void samplingIteratorCountsCachedSamplesAgainstTrimmedPopulation() {
        QuadTreeTile tile = tileWithFourQuadrantPoints();
        BitSet sampled = new BitSet(4);
        sampled.set(0);
        sampled.set(2);
        tile.setSampledTracker(sampled);

        BitSet parentOutliers = new BitSet(4);
        parentOutliers.set(2);
        tile.setOutlierData(parentOutliers, new int[] { -1, -1, 5, -1 });

        Schema schema = new Schema("unused.csv", ',', 0, 1, List.of(2),
                rectangle(0, 4, 0, 4), 4, List.of());
        Query query = new Query(rectangle(0, 4, 0, 4), List.of(2));
        QueryNode node = new QueryNode(tile, null, query, schema);

        SamplingNodePointsIterator iterator = new SamplingNodePointsIterator(node, 0.5);

        int emitted = 0;
        while (iterator.hasNext()) {
            long offset = iterator.nextOffset();
            assertTrue(offset == 200L || offset == 400L);
            emitted++;
        }

        assertEquals(1, emitted);
        assertEquals(2, node.getSampledPointCount());
    }

    private static QuadTreeTile tileWithFourQuadrantPoints() {
        double[] xs = { 1.0, 1.0, 3.0, 3.0 };
        double[] ys = { 1.0, 3.0, 1.0, 3.0 };
        long[] offsets = { 100L, 200L, 300L, 400L };
        int[] tileIds = { 0, 0, 0, 0 };
        SharedPointStore store = new SharedPointStore(
                new double[][] { xs },
                new double[][] { ys },
                new long[][] { offsets },
                new int[][] { tileIds },
                new int[] { xs.length },
                xs.length);
        store.partition(xs.length, new int[] { 0 }, 1);

        QuadTreeTile tile = new QuadTreeTile(rectangle(0, 4, 0, 4));
        tile.setSlice(store, 0, xs.length);
        return tile;
    }

    private static Rectangle rectangle(double minX, double maxX, double minY, double maxY) {
        return new Rectangle(Range.closed(minX, maxX), Range.closed(minY, maxY));
    }

    private static void assertChildOutlier(QuadTreeTile parent, long offset, int expectedOutlierIdx) {
        Tile child = childContainingOffset(parent, offset);
        assertNotNull(child.getOutlierBitSet());
        int localPos = localPosition(child, offset);
        assertTrue(child.getOutlierBitSet().get(localPos));
        assertEquals(expectedOutlierIdx, child.getOutlierIdxs()[localPos]);
    }

    private static void assertChildHasNoOutliers(QuadTreeTile parent, long offset) {
        Tile child = childContainingOffset(parent, offset);
        assertNull(child.getOutlierBitSet());
    }

    private static Tile childContainingOffset(QuadTreeTile parent, long offset) {
        for (Object obj : parent.getLeafTiles()) {
            Tile child = (Tile) obj;
            for (int i = 0; i < child.getSize(); i++) {
                if (child.getOffset(i) == offset) {
                    return child;
                }
            }
        }
        throw new AssertionError("No child contains offset " + offset);
    }

    private static int localPosition(Tile tile, long offset) {
        for (int i = 0; i < tile.getSize(); i++) {
            if (tile.getOffset(i) == offset) {
                return i;
            }
        }
        throw new AssertionError("Tile does not contain offset " + offset);
    }
}
