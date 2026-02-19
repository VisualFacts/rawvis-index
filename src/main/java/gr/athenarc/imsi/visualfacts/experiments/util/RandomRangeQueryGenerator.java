package gr.athenarc.imsi.visualfacts.experiments.util;


import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.Rectangle;

public class RandomRangeQueryGenerator {

    private Range<Double> valueRange;
    private RangeGenerator rangeGenerator;


    public RandomRangeQueryGenerator(Range<Double> valueRange) {
        this.valueRange = valueRange;
        this.rangeGenerator = new RangeGenerator(valueRange);
    }


    public Rectangle generate(double selectivity) {
        Range<Double>[] ranges = this.rangeGenerator.getEqualSizedRanges(2, selectivity);
        return new Rectangle(ranges[0], ranges[1]);
    }


    public List<Rectangle> generate(int count, double selectivity) {
        List<Rectangle> rangeQueries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rangeQueries.add(this.generate(selectivity));
        }
        return rangeQueries;
    }
}
