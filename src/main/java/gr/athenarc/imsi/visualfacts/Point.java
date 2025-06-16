package gr.athenarc.imsi.visualfacts;

import java.io.Serializable;
import java.util.List;

/**
 * Class that represents a 2d data point containing an x and y value
 */
public class Point implements Serializable {

    private float x;
    private float y;
    private long fileOffset;

    private float measure0;
    private float measure1;
    private List<String> categoricalValues;

    public Point(float x, float y, long fileOffset, float measure0, float measure1, List<String> categoricalValues) {   
        this.x = x;
        this.y = y;
        this.fileOffset = fileOffset;
        this.measure0 = measure0;
        this.measure1 = measure1;
        this.categoricalValues = categoricalValues;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public void setFileOffset(long fileOffset) {
        this.fileOffset = fileOffset;
    }

    public float getX() {
        return x;
    }

    public void setX(float x) {
        this.x = x;
    }

    public float getY() {
        return y;
    }

    public void setY(float y) {
        this.y = y;
    }

    public float getMeasure0() {
        return measure0;
    }

    public void setMeasure0(float measure0) {
        this.measure0 = measure0;
    }

    public float getMeasure1() {
        return measure1;
    }

    public void setMeasure1(float measure1) {
        this.measure1 = measure1;
    }

    public List<String> getCategoricalValues() {
        return categoricalValues;
    }

    public void setCategoricalValues(List<String> categoricalValues) {
        this.categoricalValues = categoricalValues;
    }


    @Override
    public String toString() {
        return "Point{" +
                "x=" + x +
                ", y=" + y +
                ", fileOffset=" + fileOffset +
                ", measure0=" + measure0 +
                ", measure1=" + measure1 +
                ", categoricalValues=" + categoricalValues +
                '}';
    }
}
