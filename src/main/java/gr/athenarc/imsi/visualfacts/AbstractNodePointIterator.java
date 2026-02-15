package gr.athenarc.imsi.visualfacts;

/**
 * Abstract iterator over points in a TreeNode. Subclasses implement
 * {@link #advance()} to move to the next point. The current point's
 * file offset is available via {@link #getCurrentOffset()}.
*/
public abstract class AbstractNodePointIterator {

    protected QueryNode queryNode;

    private long currentOffset = -1;
    private boolean hasAdvanced = false;
    private boolean exhausted = false;

    protected QueryNode getQueryNode() {
        return queryNode;
    }

    /**
     * Advance to the next point. Returns true if a next point exists.
     * After returning true, {@link #getCurrentOffset()} returns the offset.
     */
    protected abstract boolean advanceInternal();

    /** The file offset of the current point. Only valid after advance() returns true. */
    protected abstract long peekOffset();

    /**
     * Try to advance. Returns true if there is a next element.
     * Caches the result so multiple calls without consume() are safe.
     */
    public boolean hasNext() {
        if (!hasAdvanced && !exhausted) {
            hasAdvanced = advanceInternal();
            if (hasAdvanced) {
                currentOffset = peekOffset();
            } else {
                exhausted = true;
            }
        }
        return hasAdvanced;
    }

    /**
     * Consume the current element and return its file offset.
     */
    public long nextOffset() {
        if (!hasAdvanced) {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
        }
        hasAdvanced = false;
        return currentOffset;
    }

    /**
     * Peek at the current file offset without consuming.
     * Only valid after hasNext() returned true.
     */
    public long getCurrentOffset() {
        return currentOffset;
    }
}
