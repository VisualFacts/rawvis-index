package gr.athenarc.imsi.visualfacts;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class NodePointsPeekingIterator implements PeekingIterator<Point> {

    private PeekingIterator<Point> peekingIterator;
    private AbstractNodePointIterator nodePointsIterator;

    public NodePointsPeekingIterator(AbstractNodePointIterator nodePointsIterator) {
        this.nodePointsIterator = nodePointsIterator;
        peekingIterator = Iterators.peekingIterator(nodePointsIterator);
    }

    public QueryNode getQueryNode() {
        return nodePointsIterator.getQueryNode();
    }

    @Override
    public Point peek() {
        return peekingIterator.peek();
    }

    @Override
    public boolean hasNext() {
        return peekingIterator.hasNext();
    }

    @Override
    public Point next() {
        return peekingIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
