package gr.athenarc.imsi.visualfacts.query;

import java.util.EnumSet;
import java.util.List;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.experiments.util.UserOpType;

public class Query {

    private Rectangle rect;

    private List<Integer> measureCols;

    private UserOpType userOpType; // Added field to store the user operation

    private EnumSet<AggregateType> aggregateTypes = AggregateType.BASIC; // Aggregate functions to compute

    public Query() {
    }

    public Query(Rectangle rect, List<Integer> measureCols) {
        this.rect = rect;
        this.measureCols = measureCols;
    }

    public Query(Rectangle rect, List<Integer> measureCols, UserOpType userOpType) {
        this.rect = rect;
        this.measureCols = measureCols;
        this.userOpType = userOpType;
    }

    public Rectangle getRect() {
        return rect;
    }

    public void setRect(Rectangle rect) {
        this.rect = rect;
    }

    public List<Integer> getMeasureCols() {
        return measureCols;
    }

    public void setMeasureCols(List<Integer> measureCols) {
        this.measureCols = measureCols;
    }

    public UserOpType getUserOpType() {
        return userOpType;
    }

    public void setUserOpType(UserOpType userOpType) {
        this.userOpType = userOpType;
    }

    public EnumSet<AggregateType> getAggregateTypes() {
        return aggregateTypes;
    }

    public void setAggregateTypes(EnumSet<AggregateType> aggregateTypes) {
        this.aggregateTypes = aggregateTypes;
    }

    /**
     * Fluent setter for aggregate types.
     */
    public Query withAggregateTypes(EnumSet<AggregateType> aggregateTypes) {
        this.aggregateTypes = aggregateTypes;
        return this;
    }

    @Override
    public String toString() {
        return "Query{" +
                "op=" + userOpType +
                ", rect=" + rect +
                ", measureCols=" + measureCols +
                ", aggregateTypes=" + aggregateTypes +
                '}';
    }
}