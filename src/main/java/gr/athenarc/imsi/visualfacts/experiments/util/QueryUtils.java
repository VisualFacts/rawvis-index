package gr.athenarc.imsi.visualfacts.experiments.util;


import com.google.common.collect.Range;
import gr.athenarc.imsi.visualfacts.Rectangle;


public class QueryUtils {


    private static Rectangle moveQuery(Rectangle query, float overlap, Direction direction) {

        Range<Float> xRange = query.getXRange();
        Range<Float> yRange = query.getYRange();
        float interval;
        switch (direction) {
            case N:
                interval = (yRange.upperEndpoint() - yRange.lowerEndpoint()) * (1 - overlap);
                yRange = Range.open(yRange.lowerEndpoint() + interval, yRange.upperEndpoint() + interval);
                break;
            case S:
                interval = (yRange.upperEndpoint() - yRange.lowerEndpoint()) * (1 - overlap);
                yRange = Range.open(yRange.lowerEndpoint() - interval, yRange.upperEndpoint() - interval);
                break;
            case W:
                interval = (xRange.upperEndpoint() - xRange.lowerEndpoint()) * (1 - overlap);
                xRange = Range.open(xRange.lowerEndpoint() - interval, xRange.upperEndpoint() - interval);
                break;
            case E:
                interval = (xRange.upperEndpoint() - xRange.lowerEndpoint()) * (1 - overlap);
                xRange = Range.open(xRange.lowerEndpoint() + interval, xRange.upperEndpoint() + interval);
                break;
        }
        return new Rectangle(xRange, yRange);
    }

    public static Range<Float> convertToRange(String s) {
        String[] values = s.split(":");
        return Range.open(Float.parseFloat(values[0]), Float.parseFloat(values[1]));
    }

    public static Rectangle convertToRectangle(String s) {
        String[] ranges = s.split(",");
        String[] xValues = ranges[0].split(":");
        String[] yValues = ranges[1].split(":");
        return new Rectangle(Range.open(Float.parseFloat(xValues[0]), Float.parseFloat(xValues[1])), Range.open(Float.parseFloat(yValues[0]), Float.parseFloat(yValues[1])));
    }
}