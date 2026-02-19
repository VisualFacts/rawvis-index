package gr.athenarc.imsi.visualfacts;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.query.Query;

public class QuadTreeTile extends Tile {

    private static final Logger LOG = LogManager.getLogger(QuadTreeTile.class);

    private QuadTreeTile topLeft, topRight, bottomLeft, bottomRight;

    public QuadTreeTile(Rectangle bounds) {
        super(bounds);
    }

    @Override
    public List getLeafTiles() {
        List leafTiles = new ArrayList();
        if (this.topLeft == null) {
            leafTiles.add(this);
        } else {
            leafTiles.addAll(this.topLeft.getLeafTiles());
            leafTiles.addAll(this.topRight.getLeafTiles());
            leafTiles.addAll(this.bottomLeft.getLeafTiles());
            leafTiles.addAll(this.bottomRight.getLeafTiles());
        }
        return leafTiles;
    }

    @Override
    public List getOverlappedLeafTiles(Query query) {
        Rectangle rect = query.getRect();
        List leafTiles = new ArrayList();

        if (this.topLeft == null) {
            leafTiles.add(this);
        } else if (this.hasFrozenStats() && rect.encloses(this.bounds)) {
            // Short-circuit: this non-leaf tile has frozen exact stats and is fully
            // contained by the query. Return self as a virtual leaf — no need to
            // recurse into the subtree.
            leafTiles.add(this);
        } else {
            if (rect.intersects(this.topRight.bounds))
                leafTiles.addAll(this.topRight.getOverlappedLeafTiles(query));
            if (rect.intersects(this.bottomRight.bounds))
                leafTiles.addAll(this.bottomRight.getOverlappedLeafTiles(query));
            if (rect.intersects(this.bottomLeft.bounds))
                leafTiles.addAll(this.bottomLeft.getOverlappedLeafTiles(query));
            if (rect.intersects(this.topLeft.bounds))
                leafTiles.addAll(this.topLeft.getOverlappedLeafTiles(query));
        }
        return leafTiles;
    }

    @Override
    public List<Tile> getOverlappedActualLeafTiles(Query query) {
        Rectangle rect = query.getRect();
        List<Tile> leafTiles = new ArrayList<>();

        if (this.topLeft == null) {
            leafTiles.add(this);
        } else {
            // Always recurse to actual leaves — no frozen-stats short-circuit.
            if (rect.intersects(this.topRight.bounds))
                leafTiles.addAll(this.topRight.getOverlappedActualLeafTiles(query));
            if (rect.intersects(this.bottomRight.bounds))
                leafTiles.addAll(this.bottomRight.getOverlappedActualLeafTiles(query));
            if (rect.intersects(this.bottomLeft.bounds))
                leafTiles.addAll(this.bottomLeft.getOverlappedActualLeafTiles(query));
            if (rect.intersects(this.topLeft.bounds))
                leafTiles.addAll(this.topLeft.getOverlappedActualLeafTiles(query));
        }
        return leafTiles;
    }

    @Override
    public void split() {
        try {
            Range<Double> xRange = this.bounds.getXRange();
            double xMiddle = (xRange.upperEndpoint() + xRange.lowerEndpoint()) / 2.0;
            Range<Double> yRange = this.bounds.getYRange();
            double yMiddle = (yRange.upperEndpoint() + yRange.lowerEndpoint()) / 2.0;
            Range rangeLeft = Range.range(xRange.lowerEndpoint(), xRange.lowerBoundType(),
                    xMiddle, BoundType.CLOSED);
            Range rangeRight = Range.range(xMiddle, BoundType.OPEN, xRange.upperEndpoint(), xRange.upperBoundType());
            Range rangeBottom = Range.range(yRange.lowerEndpoint(), yRange.lowerBoundType(),
                    yMiddle, BoundType.CLOSED);
            Range rangeTop = Range.range(yMiddle, BoundType.OPEN, yRange.upperEndpoint(), yRange.upperBoundType());
            this.topLeft = new QuadTreeTile(new Rectangle(rangeLeft, rangeTop));
            this.topLeft.setCategoricalColumns(this.getCategoricalColumns());
            this.topRight = new QuadTreeTile(new Rectangle(rangeRight, rangeTop));
            this.topRight.setCategoricalColumns(this.getCategoricalColumns());
            this.bottomLeft = new QuadTreeTile(new Rectangle(rangeLeft, rangeBottom));
            this.bottomLeft.setCategoricalColumns(this.getCategoricalColumns());
            this.bottomRight = new QuadTreeTile(new Rectangle(rangeRight, rangeBottom));
            this.bottomRight.setCategoricalColumns(this.getCategoricalColumns());

            // Freeze exact stats before destroying the root node.
            this.freezeStats();

            // Sub-partition parent's slice into 4 quadrant sub-slices in-place.
            TreeNode src = this.root;
            SharedPointStore store = src.getStore();
            int parentStart = src.getStart();
            int n = src.getSize();

            // Count per quadrant and assign quadrant IDs
            int[] counts = new int[4]; // 0=BL, 1=TL, 2=BR, 3=TR
            int[] subIds = new int[n];
            for (int i = 0; i < n; i++) {
                int q = (store.getX(parentStart + i) <= xMiddle ? 0 : 2)
                      | (store.getY(parentStart + i) <= yMiddle ? 0 : 1);
                subIds[i] = q;
                counts[q]++;
            }

            // Compute sub-starts (absolute positions within the shared store)
            int[] subStarts = new int[4];
            subStarts[0] = parentStart;
            for (int q = 1; q < 4; q++) {
                subStarts[q] = subStarts[q - 1] + counts[q - 1];
            }

            // In-place sub-partition
            store.subPartition(parentStart, n, subIds, subStarts, 4);

            // Wire children to sub-slices
            QuadTreeTile[] quads = { this.bottomLeft, this.topLeft, this.bottomRight, this.topRight };
            for (int q = 0; q < 4; q++) {
                if (counts[q] > 0) {
                    TreeNode childRoot = quads[q].getOrCreateRoot();
                    childRoot.setSlice(store, subStarts[q], counts[q]);
                }
            }

            this.root = null;
        } catch (IllegalArgumentException e){
            LOG.debug(e);
            LOG.debug("Unable to split");
        }
    }

    @Override
    public Tile getLeafTile(double x, double y) {
        if (this.topLeft == null) {
            return this;
        } else {
            boolean left = x <= this.bottomLeft.bounds.getXRange().upperEndpoint();
            boolean bottom = y <= this.bottomLeft.bounds.getYRange().upperEndpoint();
            QuadTreeTile tmp;
            if (left) {
                tmp = bottom ? this.bottomLeft : this.topLeft;
            } else {
                tmp = bottom ? this.bottomRight : this.topRight;
            }
            return tmp;
        }
    }

    @Override
    public int getLeafTileCount() {
        if (topLeft == null) {
            return 1;
        }
        return topLeft.getLeafTileCount() + topRight.getLeafTileCount()
                + bottomLeft.getLeafTileCount() + bottomRight.getLeafTileCount();
    }

    @Override
    public int getMaxDepth() {
        if (topLeft == null) {
            return 0;
        }
        int depth = 0;
        depth = Integer.max(depth, topLeft.getMaxDepth() + 1);
        depth = Integer.max(depth, topRight.getMaxDepth() + 1);
        depth = Integer.max(depth, bottomLeft.getMaxDepth() + 1);
        depth = Integer.max(depth, bottomRight.getMaxDepth() + 1);
        return depth;
    }
}