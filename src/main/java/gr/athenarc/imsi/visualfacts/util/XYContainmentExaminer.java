package gr.athenarc.imsi.visualfacts.util;

import com.google.common.collect.Range;

public class XYContainmentExaminer implements ContainmentExaminer {

    private Range<Double> xRange;
    private Range<Double> yRange;

    public XYContainmentExaminer(Range<Double> xRange, Range<Double> yRange) {
        this.xRange = xRange;
        this.yRange = yRange;
    }

    @Override
    public boolean contains(double x, double y) {
        return xRange.contains(x) && yRange.contains(y);
    }
}
