package gr.athenarc.imsi.visualfacts.experiments.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.experiments.util.QueryUtils;

/**
 * Configuration class for the initial query (q0) in an exploration scenario.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InitialQueryConfig {

    @JsonProperty("rect")
    private String rect;

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

    @Override
    public String toString() {
        return "InitialQueryConfig{" +
                "rect='" + rect + '\'' +
                '}';
    }
}
