package gr.athenarc.imsi.visualfacts.util;

import com.google.common.collect.Range;

public class XContainmentExaminer implements ContainmentExaminer {

    private Range<Double> xRange;

    public XContainmentExaminer(Range<Double> xRange) {
        this.xRange = xRange;
    }

    @Override
    public boolean contains(double x, double y) {
        return xRange.contains(x);
    }
}
