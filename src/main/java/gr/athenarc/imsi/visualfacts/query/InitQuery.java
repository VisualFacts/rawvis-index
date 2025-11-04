package gr.athenarc.imsi.visualfacts.query;

import gr.athenarc.imsi.visualfacts.Rectangle;

import java.util.List;
import java.util.Map;

public class InitQuery {

    private Rectangle rect;

    // map from column index to filter value
    private Map<Integer, String> categoricalFilters;

    private List<Integer> groupByCols;

    private Integer measureCol0;

    private Integer measureCol1;

    public InitQuery() {
    }

    public InitQuery(Rectangle rect, Map<Integer, String> categoricalFilters, List<Integer> groupByCols, Integer measureCol0, Integer measureCol1) {
        this.rect = rect;
        this.categoricalFilters = categoricalFilters;
        this.groupByCols = groupByCols;
        this.measureCol0 = measureCol0;
        this.measureCol1 = measureCol1;
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

    public Integer getMeasureCol0() {
        return measureCol0;
    }

    public void setMeasureCol0(Integer measureCol0) {
        this.measureCol0 = measureCol0;
    }

    public Integer getMeasureCol1() {
        return measureCol1;
    }

    public void setMeasureCol1(Integer measureCol1) {
        this.measureCol1 = measureCol1;
    }

    @Override
    public String toString() {
        return "Query{" +
                "rect=" + rect +
                ", categoricalFilters=" + categoricalFilters +
                ", groupByCols=" + groupByCols +
                ", measureCol0=" + measureCol0 +
                ", measureCol1=" + measureCol1 +
                '}';
    }
}