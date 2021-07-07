package gr.athenarc.imsi.visualfacts.queryER.Comparators;

import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import java.util.Comparator;

public class BlockCardinalityComparator implements Comparator<AbstractBlock> {

    @Override
    public int compare(AbstractBlock block1, AbstractBlock block2) {
        return new Double(block1.getNoOfComparisons()).compareTo(block2.getNoOfComparisons());
    }
}