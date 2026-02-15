package gr.athenarc.imsi.visualfacts.util;

import com.google.common.collect.Range;

public class XYContainmentExaminer implements ContainmentExaminer {

    private Range<Float> xRange;
    private Range<Float> yRange;

    public XYContainmentExaminer(Range<Float> xRange, Range<Float> yRange) {
        this.xRange = xRange;
        this.yRange = yRange;
    }

    @Override
    public boolean contains(float x, float y) {
        return xRange.contains(x) && yRange.contains(y);
    }
}
