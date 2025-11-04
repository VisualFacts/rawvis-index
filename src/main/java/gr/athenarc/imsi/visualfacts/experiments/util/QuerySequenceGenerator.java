package gr.athenarc.imsi.visualfacts.experiments.util;

import com.google.common.collect.Range;
import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.query.InitQuery;
import gr.athenarc.imsi.visualfacts.query.Query;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static gr.athenarc.imsi.visualfacts.experiments.util.UserOpType.*;

public class QuerySequenceGenerator {

    private static final Logger LOG = LogManager.getLogger(QuerySequenceGenerator.class);

    private int minShift;
    private int maxShift;

    private int minFilters;
    private int maxFilters;

    private float zoomFactor;

    public QuerySequenceGenerator(int minShift, int maxShift, int minFilters, int maxFilters, float zoomFactor) {
        this.minShift = minShift;
        this.maxShift = maxShift;
        this.minFilters = minFilters;
        this.maxFilters = maxFilters;
        this.zoomFactor = zoomFactor;
    }

    public List<Query> generateQuerySequence(InitQuery q0, int count, Schema schema) {
        Query query0 = new Query(q0.getRect(), q0.getCategoricalFilters(), q0.getGroupByCols(), q0.getMeasureCol0());
        Integer measureCol0 = q0.getMeasureCol0();
        Integer measureCol1 = q0.getMeasureCol1();
        Direction[] directions = Direction.getRandomDirections(count);
        int[] shifts = new Random(0).ints(count, minShift, maxShift + 1).toArray();
        int[] filterCounts = new Random(0).ints(count, minFilters, maxFilters + 1).toArray();

        List<Pair<CategoricalColumn, Double>> catColPairs = new ArrayList<>();
        for (CategoricalColumn categoricalColumn : schema.getCategoricalColumns()) {
            if (query0.getGroupByCols() != null && !query0.getGroupByCols().isEmpty() && !query0.getGroupByCols().contains(categoricalColumn.getIndex())) {
                catColPairs.add(new Pair<>(categoricalColumn, categoricalColumn.getScore(query0)));
            }
        }
        Random opRand = new Random(0);
        List<UserOpType> ops = Arrays.asList(new UserOpType[]{P, P, ZI, ZO});

        EnumeratedDistribution<CategoricalColumn> colDistribution = null;
        if (!catColPairs.isEmpty()) {
            colDistribution = new EnumeratedDistribution<>(catColPairs);
            colDistribution.reseedRandomGenerator(0);
        }

        Random randomFilterValueGen = new Random(0);
        List<Query> queries = new ArrayList<>();
        queries.add(query0);
        Query query = query0;
        for (int i = 0; i < count - 1; i++) {
            UserOpType opType = ops.get(opRand.nextInt(ops.size()));
            Rectangle rect;
            if (zoomFactor > 1 && opType.equals(ZI)) {
                rect = zoomIn(query);
            } else if (zoomFactor > 1 && opType.equals(ZO)) {
                rect = zoomOut(query);
            } else {
                rect = pan(query, shifts[i], directions[i]);
            }

            Map<Integer, String> filters = new HashMap<>();
            int filterCount = filterCounts[i];

            while (colDistribution != null && filterCount > 0) {
                CategoricalColumn column = colDistribution.sample();
                if (!filters.containsKey(column.getIndex())) {
                    String filterValue = column.getValue((short) randomFilterValueGen.nextInt(column.getCardinality()));
                    filters.put(column.getIndex(), filterValue);
                    filterCount--;
                }
            }
            int measureCol = measureCol0;
            if(measureCol1 != null && i % 2 == 1) {
                measureCol = measureCol1;
            }
            query = new Query(rect, filters, q0.getGroupByCols(), measureCol);
            queries.add(query);
        }
        return queries;
    }

    private Rectangle pan(Query query, int shift, Direction direction) {
        Range<Float> xRange = query.getRect().getXRange();
        Range<Float> yRange = query.getRect().getYRange();
        shift = Math.abs(shift);

        switch (direction) {
            case N:
            case NE:
            case NW:
                yRange = adjustRange(yRange, shift);
                break;
            case S:
            case SE:
            case SW:
                yRange = adjustRange(yRange, -shift);
        }
        switch (direction) {
            case E:
            case NE:
            case SE:
                xRange = adjustRange(xRange, shift);
                break;
            case W:
            case NW:
            case SW:
                xRange = adjustRange(xRange, -shift);
        }
        return new Rectangle(xRange, yRange);
    }

    private Rectangle zoomOut(Query query) {
        return zoom(query, zoomFactor);
    }

    private Rectangle zoomIn(Query query) {
        return zoom(query, 1f / zoomFactor);
    }

    private Rectangle zoom(Query query, float zoomFactor) {
        Range<Float> xRange = query.getRect().getXRange();
        Range<Float> yRange = query.getRect().getYRange();

        float xMiddle = (xRange.upperEndpoint() + xRange.lowerEndpoint()) / 2f;
        float yMiddle = (yRange.upperEndpoint() + yRange.lowerEndpoint()) / 2f;
        float newXSize = (xRange.upperEndpoint() - xRange.lowerEndpoint()) * zoomFactor;
        float newYSize = (yRange.upperEndpoint() - yRange.lowerEndpoint()) * zoomFactor;

        return new Rectangle(Range.open(xMiddle - (newXSize / 2f), xMiddle + (newXSize / 2f)),
                Range.open(yMiddle - (newYSize / 2f), yMiddle + (newYSize / 2f)));
    }

    private Range<Float> adjustRange(Range<Float> range, int shift) {
        float interval = (range.upperEndpoint() -
                range.lowerEndpoint()) * shift / 100;
        return Range.open(range.lowerEndpoint() + interval, range.upperEndpoint() + interval);
    }

}
