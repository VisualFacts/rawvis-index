package gr.athenarc.imsi.visualfacts.util;

import com.google.common.collect.Range;

public class YContainmentExaminer implements ContainmentExaminer {

    private Range<Float> yRange;

    public YContainmentExaminer(Range<Float> yRange) {
        this.yRange = yRange;
    }

    @Override
    public boolean contains(float x, float y) {
        return yRange.contains(y);
    }
}
