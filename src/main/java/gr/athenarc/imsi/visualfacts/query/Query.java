package gr.athenarc.imsi.visualfacts.query;

import gr.athenarc.imsi.visualfacts.Rectangle;

import java.util.List;
import java.util.Map;

public class Query {

    private Rectangle rect;

    // map from column index to filter value
    private Map<Integer, String> categoricalFilters;

    private List<Integer> groupByCols;

    private Integer measureCol;

    public Query() {
    }

    public Query(Rectangle rect, Map<Integer, String> categoricalFilters, List<Integer> groupByCols, Integer measureCol) {
        this.rect = rect;
        this.categoricalFilters = categoricalFilters;
        this.groupByCols = groupByCols;
        this.measureCol = measureCol;
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

    public Integer getMeasureCol() {
        return measureCol;
    }

    public void setMeasureCol(Integer measureCol) {
        this.measureCol = measureCol;
    }

    @Override
    public String toString() {
        return "Query{" +
                "rect=" + rect +
                ", categoricalFilters=" + categoricalFilters +
                ", groupByCols=" + groupByCols +
                ", measureCol=" + measureCol +
                '}';
    }
}