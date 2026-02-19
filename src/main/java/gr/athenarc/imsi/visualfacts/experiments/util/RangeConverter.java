package gr.athenarc.imsi.visualfacts.experiments.util;

import com.beust.jcommander.IStringConverter;
import com.google.common.collect.Range;


public class RangeConverter implements IStringConverter<Range<Double>> {

    @Override
    public Range<Double> convert(String s) {
        return QueryUtils.convertToRange(s);
    }
}
