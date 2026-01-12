package gr.athenarc.imsi.visualfacts.query;

import java.util.ArrayList;
import java.util.HashMap;
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

    /**
     * Serializes the Query to a string format that can be persisted and restored.
     * Format: rect|categoricalFilters|groupByCols|measureCols|userOpType
     */
    public String toSerializedString() {
        StringBuilder sb = new StringBuilder();
        
        // Serialize Rectangle
        if (rect != null) {
            sb.append(rect.toString());
        } else {
            sb.append("null");
        }
        sb.append("|");
        
        // Serialize categorical filters
        if (categoricalFilters != null && !categoricalFilters.isEmpty()) {
            for (Map.Entry<Integer, String> entry : categoricalFilters.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            if (sb.charAt(sb.length() - 1) == ',') {
                sb.deleteCharAt(sb.length() - 1);
            }
        } else {
            sb.append("null");
        }
        sb.append("|");
        
        // Serialize groupByCols
        if (groupByCols != null && !groupByCols.isEmpty()) {
            for (int i = 0; i < groupByCols.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(groupByCols.get(i));
            }
        } else {
            sb.append("null");
        }
        sb.append("|");
        
        // Serialize measureCols
        if (measureCols != null && !measureCols.isEmpty()) {
            for (int i = 0; i < measureCols.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(measureCols.get(i));
            }
        } else {
            sb.append("null");
        }
        sb.append("|");
        
        // Serialize userOpType
        sb.append(userOpType != null ? userOpType.toString() : "null");
        
        return sb.toString();
    }

    /**
     * Deserializes a Query from a string format created by toSerializedString().
     */
    public static Query fromSerializedString(String serialized) {
        String[] parts = serialized.split("\\|", -1);
        if (parts.length != 5) {
            throw new IllegalArgumentException("Invalid serialized query format: " + serialized);
        }
        
        // Deserialize Rectangle
        Rectangle rect = parts[0].equals("null") ? null : Rectangle.fromString(parts[0]);
        
        // Deserialize categorical filters
        Map<Integer, String> categoricalFilters = null;
        if (!parts[1].equals("null") && !parts[1].isEmpty()) {
            categoricalFilters = new HashMap<>();
            String[] filterPairs = parts[1].split(",");
            for (String pair : filterPairs) {
                String[] kv = pair.split(":", 2);
                if (kv.length == 2) {
                    categoricalFilters.put(Integer.parseInt(kv[0]), kv[1]);
                }
            }
        }
        
        // Deserialize groupByCols
        List<Integer> groupByCols = null;
        if (!parts[2].equals("null") && !parts[2].isEmpty()) {
            groupByCols = new ArrayList<>();
            String[] cols = parts[2].split(",");
            for (String col : cols) {
                groupByCols.add(Integer.parseInt(col));
            }
        }
        
        // Deserialize measureCols
        List<Integer> measureCols = null;
        if (!parts[3].equals("null") && !parts[3].isEmpty()) {
            measureCols = new ArrayList<>();
            String[] cols = parts[3].split(",");
            for (String col : cols) {
                measureCols.add(Integer.parseInt(col));
            }
        }
        
        // Deserialize userOpType
        UserOpType userOpType = parts[4].equals("null") ? null : UserOpType.valueOf(parts[4]);
        
        return new Query(rect, categoricalFilters, groupByCols, measureCols, userOpType);
    }
}