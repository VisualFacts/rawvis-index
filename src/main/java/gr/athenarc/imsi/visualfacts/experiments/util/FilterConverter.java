package gr.athenarc.imsi.visualfacts.experiments.util;

import com.beust.jcommander.IStringConverter;

import java.util.HashMap;
import java.util.Map;


public class FilterConverter implements IStringConverter<Map<Integer, String>> {

    @Override
    public Map<Integer, String> convert(String s) {
        Map<Integer, String> filterMap = new HashMap<>();
        String[] filters = s.split(",");
        for (String filter : filters) {
            String[] filterParts = filter.split(":");
            filterMap.put(Integer.parseInt(filterParts[0]), filterParts[1]);
        }
        return filterMap;
    }
}
