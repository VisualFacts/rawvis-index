package gr.athenarc.imsi.visualfacts;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Range;


public class Rectangle implements Serializable {

    private final Range<Double> xRange;
    private final Range<Double> yRange;

    public Rectangle(Range<Double> xRange, Range<Double> yRange) {
        this.xRange = xRange;
        this.yRange = yRange;
    }

    public Range<Double> getXRange() {
        return xRange;
    }

    public Range<Double> getYRange() {
        return yRange;
    }

    public boolean contains(double x, double y) {
        return xRange.contains(x) && yRange.contains(y);
    }

    public boolean intersects(Rectangle other) {
        return this.xRange.isConnected(other.getXRange()) && !this.xRange.intersection(other.getXRange()).isEmpty()
                && this.yRange.isConnected(other.getYRange()) && !this.yRange.intersection(other.getYRange()).isEmpty();
    }

    public boolean encloses(Rectangle other) {
        return this.xRange.encloses(other.getXRange()) && this.yRange.encloses(other.getYRange());
    }

    public double getCenterX() {
        return (xRange.lowerEndpoint() + xRange.upperEndpoint())/2d;
    }

    public double getCenterY() {
        return (yRange.lowerEndpoint() + yRange.upperEndpoint())/2d;
    }

    public double getXSize() {
        return xRange.upperEndpoint() - xRange.lowerEndpoint();
    }

    public double getYSize() {
        return yRange.upperEndpoint() - yRange.lowerEndpoint();
    }

    public List toList() {
        List<Range<Double>> list = new ArrayList<>(2);
        list.add(this.xRange);
        list.add(this.yRange);
        return list;
    }

    public double distanceFrom(Rectangle other){
        double centerX = (xRange.lowerEndpoint() + xRange.upperEndpoint()) / 2d;
        double centerY = (yRange.lowerEndpoint() + yRange.upperEndpoint()) / 2d;
        double otherCenterX = (other.xRange.lowerEndpoint() + other.xRange.upperEndpoint()) / 2d;
        double otherCenterY = (other.yRange.lowerEndpoint() + other.yRange.upperEndpoint()) / 2d;
        return Math.hypot(Math.abs(centerX - otherCenterX), Math.abs(centerY - otherCenterY));
    }

    public double getSurfaceArea(){
        return getXSize() * getYSize();
    }


    @Override
    public String toString() {
        return xRange.toString() + "," + yRange.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rectangle rectangle = (Rectangle) o;
        return Objects.equals(xRange, rectangle.xRange) &&
                Objects.equals(yRange, rectangle.yRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(xRange, yRange);
    }

    /**
     * Deserializes a Rectangle from a string format created by toString().
     * Format: xRange,yRange (e.g., "(1.0..2.0),[3.0..4.0)")
     */
    public static Rectangle fromString(String str) {
        String[] parts = str.split(",", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid rectangle format: " + str);
        }
        
        Range<Double> xRange = parseRange(parts[0]);
        Range<Double> yRange = parseRange(parts[1]);
        
        return new Rectangle(xRange, yRange);
    }

    /**
     * Helper method to parse Range from string format.
     * Handles formats like "(1.0..2.0)", "[1.0..2.0]", "(1.0..2.0]", "[1.0..2.0)"
     */
    private static Range<Double> parseRange(String rangeStr) {
        rangeStr = rangeStr.trim();
        
        boolean leftOpen = rangeStr.startsWith("(");
        boolean rightOpen = rangeStr.endsWith(")");
        
        // Remove brackets
        String content = rangeStr.substring(1, rangeStr.length() - 1);
        String[] values = content.split("\\.\\.");
        if (values.length != 2) {
            throw new IllegalArgumentException("Invalid range format: " + rangeStr);
        }
        
        Double lower = Double.parseDouble(values[0]);
        Double upper = Double.parseDouble(values[1]);
        
        if (leftOpen && rightOpen) {
            return Range.open(lower, upper);
        } else if (leftOpen && !rightOpen) {
            return Range.openClosed(lower, upper);
        } else if (!leftOpen && rightOpen) {
            return Range.closedOpen(lower, upper);
        } else {
            return Range.closed(lower, upper);
        }
    }
}
