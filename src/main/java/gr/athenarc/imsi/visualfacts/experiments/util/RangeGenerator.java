package gr.athenarc.imsi.visualfacts.experiments.util;

import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Range;


public class RangeGenerator {

    private Range<Double> valueRange;

    public RangeGenerator(Range<Double> valueRange) {
        this.valueRange = valueRange;
    }

    public Range<Double>[] getEqualSizedRanges(int rangeCount, double totalSelectivity) {
        Range<Double>[] ranges = new Range[rangeCount];
        double rangeSize = (valueRange.upperEndpoint() - valueRange.lowerEndpoint()) * Math.pow(totalSelectivity, 1.0 / rangeCount);
        double totalMax = valueRange.upperEndpoint() - rangeSize;
        for (int i = 0; i < rangeCount; i++) {
            double rangeMin = ThreadLocalRandom.current().nextDouble(valueRange.lowerEndpoint(), totalMax);
            double rangeMax = rangeMin + rangeSize;
            ranges[i] = Range.open(rangeMin, rangeMax);
        }
        return ranges;
    }

    public Range<Double> getValueRange() {
        return valueRange;
    }

    public void setValueRange(Range<Double> valueRange) {
        this.valueRange = valueRange;
    }
}
