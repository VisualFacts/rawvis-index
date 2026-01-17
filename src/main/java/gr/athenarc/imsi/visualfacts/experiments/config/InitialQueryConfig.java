package gr.athenarc.imsi.visualfacts.experiments.config;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.experiments.util.QueryUtils;

/**
 * Configuration class for the initial query (q0) in an exploration scenario.
 */
public class InitialQueryConfig {

    @JsonProperty("rect")
    private String rect;

    @JsonProperty("filters")
    private Map<Integer, String> filters = new HashMap<>();

    // Default constructor for Jackson
    public InitialQueryConfig() {
    }

    /**
     * Converts the rect string to a Rectangle object.
     */
    public Rectangle toRectangle() {
        return QueryUtils.convertToRectangle(rect);
    }

    public String getRect() {
        return rect;
    }

    public void setRect(String rect) {
        this.rect = rect;
    }

    public Map<Integer, String> getFilters() {
        return filters;
    }

    public void setFilters(Map<Integer, String> filters) {
        this.filters = filters;
    }

    @Override
    public String toString() {
        return "InitialQueryConfig{" +
                "rect='" + rect + '\'' +
                ", filters=" + filters +
                '}';
    }
}
