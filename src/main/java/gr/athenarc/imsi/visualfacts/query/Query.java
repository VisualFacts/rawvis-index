package gr.athenarc.imsi.visualfacts.query;

import java.util.List;
import java.util.Map;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.experiments.util.UserOpType;

public class Query {

    private Rectangle rect;

    // map from column index to filter value
    private Map<Integer, String> categoricalFilters;

    private List<Integer> groupByCols;

    private List<Integer> measureCols;

    private UserOpType userOpType; // Added field to store the user operation

    public Query() {
    }

    public Query(Rectangle rect, Map<Integer, String> categoricalFilters, List<Integer> groupByCols,
            List<Integer> measureCols) {
        this.rect = rect;
        this.categoricalFilters = categoricalFilters;
        this.groupByCols = groupByCols;
        this.measureCols = measureCols;
        this.userOpType = null; // Default to null for initial or ad hoc queries
    }

    public Query(Rectangle rect, Map<Integer, String> categoricalFilters, List<Integer> groupByCols,
            List<Integer> measureCols, UserOpType userOpType) {
        this.rect = rect;
        this.categoricalFilters = categoricalFilters;
        this.groupByCols = groupByCols;
        this.measureCols = measureCols;
        this.userOpType = userOpType;
    }

    public Rectangle getRect() {
        return rect;
    }

    public void setRect(Rectangle rect) {
        this.rect = rect;
    }

    public Map<Integer, String> getCategoricalFilters() {
        return categoricalFilters;
    }

    public void setCategoricalFilters(Map<Integer, String> categoricalFilters) {
        this.categoricalFilters = categoricalFilters;
    }

    public List<Integer> getGroupByCols() {
        return groupByCols;
    }

    public void setGroupByCols(List<Integer> groupByCols) {
        this.groupByCols = groupByCols;
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

    @Override
    public String toString() {
        return "Query{" +
                "op=" + userOpType +
                ", rect=" + rect +
                ", categoricalFilters=" + categoricalFilters +
                ", groupByCols=" + groupByCols +
                ", measureCols=" + measureCols +
                '}';
    }
}