package gr.athenarc.imsi.visualfacts.util;

import com.google.common.collect.Range;

public class YContainmentExaminer implements ContainmentExaminer {

    private Range<Double> yRange;

    public YContainmentExaminer(Range<Double> yRange) {
        this.yRange = yRange;
    }

    @Override
    public boolean contains(double x, double y) {
        return yRange.contains(y);
    }
}
