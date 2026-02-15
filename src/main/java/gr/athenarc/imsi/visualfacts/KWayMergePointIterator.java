package gr.athenarc.imsi.visualfacts;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * K-way merge iterator that produces points in file-offset order
 * from multiple node iterators.
 */
public class KWayMergePointIterator {

    private QueryNode currentQueryNode;
    private PriorityQueue<AbstractNodePointIterator> pq;
    private long currentOffset;

    public KWayMergePointIterator(List<? extends AbstractNodePointIterator> nodePointsIterators) {
        Comparator<AbstractNodePointIterator> comparator =
                Comparator.comparingLong(AbstractNodePointIterator::getCurrentOffset);
        pq = new PriorityQueue<>(Math.max(nodePointsIterators.size(), 1), comparator);
        for (AbstractNodePointIterator it : nodePointsIterators) {
            if (it.hasNext()) {
                pq.add(it);
            }
        }
    }

    public boolean hasNext() {
        return !pq.isEmpty();
    }

    /**
     * Returns the file offset of the next point in merge order.
     * After calling this, {@link #getCurrentQueryNode()} returns the
     * QueryNode that owns this point.
     */
    public long nextOffset() {
        AbstractNodePointIterator it = pq.poll();
        currentOffset = it.nextOffset();
        currentQueryNode = it.getQueryNode();
        if (it.hasNext()) {
            pq.add(it);
        }
        return currentOffset;
    }

    public QueryNode getCurrentQueryNode() {
        return currentQueryNode;
    }
}
