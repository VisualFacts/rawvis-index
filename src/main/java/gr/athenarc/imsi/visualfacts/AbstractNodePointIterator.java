package gr.athenarc.imsi.visualfacts;

import java.util.Iterator;
import java.util.NoSuchElementException;


public abstract class AbstractNodePointIterator implements Iterator<Point> {

    private Point current;

    protected QueryNode queryNode;

    protected abstract Point getNext();

    protected QueryNode getQueryNode() {
        return queryNode;
    }


    @Override
    public boolean hasNext() {
        if (this.current == null) {
            this.current = this.getNext();
        }
        return this.current != null;
    }

    @Override
    public Point next() {
        Point next = this.current;
        this.current = null;

        if (next == null) {
            // hasNext() wasn't called before
            next = this.getNext();
            if (next == null) {
                throw new NoSuchElementException("No more data entries available");
            }
        }
        return next;
    }

}
