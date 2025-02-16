package gr.athenarc.imsi.visualfacts;

import gr.athenarc.imsi.visualfacts.query.FilterPredicate;

public class DataValidationFilter {
    private int filterColumn;
    private FilterPredicate filterPredicate;

    public DataValidationFilter(int filterColumn, FilterPredicate filterPredicate) {
        this.filterColumn = filterColumn;
        this.filterPredicate = filterPredicate;
    }

    public boolean test(Double value) {
        return filterPredicate.test(value);
    }

    public int getFilterColumn() {
        return filterColumn;
    }

    public void setFilterColumn(int filterColumn) {
        this.filterColumn = filterColumn;
    }

    public FilterPredicate getFilterPredicate() {
        return filterPredicate;
    }

    public void setFilterPredicate(FilterPredicate filterPredicate) {
        this.filterPredicate = filterPredicate;
    }

    @Override
    public String toString() {
        return "DataValidationFilter{" +
                "filterColumn=" + filterColumn +
                ", filterPredicate=" + filterPredicate +
                '}';
    }
}
