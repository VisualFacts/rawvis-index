package gr.athenarc.imsi.visualfacts.tests.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.experiments.util.QuerySequenceGenerator;
import gr.athenarc.imsi.visualfacts.query.Query;

public final class TestScenarioGenerator {
    private TestScenarioGenerator() {
    }

    public static List<Query> generate(Schema schema, int count) {
        Rectangle q0Rect = new Rectangle(Range.closed(544f, 574f), Range.closed(323f, 353f));
        Query q0 = new Query(q0Rect, java.util.Collections.<Integer, String>emptyMap(),
                java.util.Collections.<Integer>emptyList(), schema.getMeasureCols());

        int minShift = 10, maxShift = 20;
        int minFilters = 0, maxFilters = 0;
        float zoomFactor = 1.2f;
        QuerySequenceGenerator gen = new QuerySequenceGenerator(minShift, maxShift, minFilters, maxFilters, zoomFactor);
        List<Query> queries = gen.generateQuerySequence(q0, count, schema);
        return queries;
    }
}
