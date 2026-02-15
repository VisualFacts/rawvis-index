package gr.athenarc.imsi.visualfacts.util;

import com.google.common.collect.Range;

public class XContainmentExaminer implements ContainmentExaminer {

    private Range<Float> xRange;

    public XContainmentExaminer(Range<Float> xRange) {
        this.xRange = xRange;
    }

    @Override
    public boolean contains(float x, float y) {
        return xRange.contains(x);
    }
}
