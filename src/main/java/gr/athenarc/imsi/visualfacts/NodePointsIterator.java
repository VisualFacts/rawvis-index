package gr.athenarc.imsi.visualfacts;

import java.util.BitSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Iterates over points in a QueryNode that match the containment examiner.
 * For fully-contained tiles (no containment examiner), iterates all points.
 * Uses pre-computed queryPointsBitSet from QueryNode for efficient traversal.
 */
public class NodePointsIterator extends AbstractNodePointIterator {
    private int currentIndex = -1;
    private final BitSet queryPointsBitSet;
    private static final Logger LOG = LogManager.getLogger(NodePointsIterator.class);

    public NodePointsIterator(QueryNode queryNode) {
        this.queryNode = queryNode;
        this.queryPointsBitSet = queryNode.getQueryPointsBitSet();
    }

    @Override
    protected boolean advanceInternal() {
        currentIndex = queryPointsBitSet.nextSetBit(currentIndex + 1);
        return currentIndex >= 0;
    }

    @Override
    protected long peekOffset() {
        return queryNode.getTile().getOffset(currentIndex);
    }

    public QueryNode getQueryNode() {
        return queryNode;
    }
}