package gr.athenarc.imsi.visualfacts.tests.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import com.google.common.math.Stats;

import gr.athenarc.imsi.visualfacts.query.QueryResults;

public final class ResultsComparator {
    private ResultsComparator() {}

    public static void assertClose(QueryResults expected, QueryResults actual, double relTol) {
        Map<Integer, Stats> exp = expected.getRectStats();
        Map<Integer, Stats> act = actual.getRectStats();
        assertThat(exp)
            .as("expected rectStats must be present")
            .isNotNull();
        assertThat(act)
            .as("actual rectStats must be present")
            .isNotNull();

        assertThat(act.keySet()).as("measures present").isEqualTo(exp.keySet());
        for (Integer measure : exp.keySet()) {
            Stats es = exp.get(measure);
            Stats as = act.get(measure);
            
            double eSum = es == null ? 0.0 : es.sum();
            double aSum = as == null ? 0.0 : as.sum();
            double denom = Math.max(1e-9, Math.abs(eSum));
            double relErr = Math.abs(aSum - eSum) / denom;
            assertThat(relErr)
                    .as("relative sum error for measure %s (expected=%s actual=%s)", measure, eSum, aSum)
                    .isLessThanOrEqualTo(relTol);
        }
    }
}
