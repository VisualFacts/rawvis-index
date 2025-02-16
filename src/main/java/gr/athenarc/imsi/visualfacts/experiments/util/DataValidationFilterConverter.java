package gr.athenarc.imsi.visualfacts.experiments.util;

import com.beust.jcommander.IStringConverter;
import gr.athenarc.imsi.visualfacts.DataValidationFilter;
import gr.athenarc.imsi.visualfacts.query.FilterOperator;
import gr.athenarc.imsi.visualfacts.query.FilterPredicate;


public class DataValidationFilterConverter implements IStringConverter<DataValidationFilter> {

    @Override
    public DataValidationFilter convert(String value) {
        value = value.trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Empty validation filter provided.");
        }

        // Extract column, operator, and value
        String[] parts = value.split("(?=[<>=!])", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid filter format: " + value);
        }

        int columnIndex = Integer.parseInt(parts[0].trim());
        String operatorWithValue = parts[1].trim();

        // Extract operator and constant value
        FilterOperator operator = extractOperator(operatorWithValue);
        double constant = Double.parseDouble(operatorWithValue.replaceAll("[^0-9.]", ""));

        return new DataValidationFilter(columnIndex, new FilterPredicate(operator, constant));
    }

    private FilterOperator extractOperator(String condition) {
        if (condition.startsWith("<=")) return FilterOperator.LESS_THAN_OR_EQUAL;
        if (condition.startsWith(">=")) return FilterOperator.GREATER_THAN_OR_EQUAL;
        if (condition.startsWith("==")) return FilterOperator.EQUAL;
        if (condition.startsWith("!=")) return FilterOperator.NOT_EQUAL;
        if (condition.startsWith("<")) return FilterOperator.LESS_THAN;
        if (condition.startsWith(">")) return FilterOperator.GREATER_THAN;
        throw new IllegalArgumentException("Invalid operator in condition: " + condition);
    }
}
