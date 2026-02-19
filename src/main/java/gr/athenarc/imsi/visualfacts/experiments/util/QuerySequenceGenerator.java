package gr.athenarc.imsi.visualfacts.experiments.util;

import static gr.athenarc.imsi.visualfacts.experiments.util.UserOpType.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.query.Query;

public class QuerySequenceGenerator {

    private static final Logger LOG = LogManager.getLogger(QuerySequenceGenerator.class);

    private int minShift;
    private int maxShift;

    private int minFilters;
    private int maxFilters;

    private double zoomFactor;

    private Map<String, Double> directionWeights;

    public QuerySequenceGenerator(int minShift, int maxShift, int minFilters, int maxFilters, double zoomFactor) {
        this(minShift, maxShift, minFilters, maxFilters, zoomFactor, null);
    }

    public QuerySequenceGenerator(int minShift, int maxShift, int minFilters, int maxFilters, double zoomFactor,
                                  Map<String, Double> directionWeights) {
        this.minShift = minShift;
        this.maxShift = maxShift;
        this.minFilters = minFilters;
        this.maxFilters = maxFilters;
        this.zoomFactor = zoomFactor;
        this.directionWeights = directionWeights;
    }

    public List<Query> generateQuerySequence(Query q0, int count, Schema schema) {

        Direction[] directions = Direction.getRandomDirections(count, directionWeights);
        int[] shifts = new Random(0).ints(count, minShift, maxShift + 1).toArray();
        int[] filterCounts = new Random(0).ints(count, minFilters, maxFilters + 1).toArray();

        List<Pair<CategoricalColumn, Double>> catColPairs = new ArrayList<>();
        for (CategoricalColumn categoricalColumn : schema.getCategoricalColumns()) {
            if (q0.getGroupByCols() != null && !q0.getGroupByCols().isEmpty() && !q0.getGroupByCols().contains(categoricalColumn.getIndex())) {
                catColPairs.add(new Pair<>(categoricalColumn, categoricalColumn.getScore(q0)));
            }
        }
        Random opRand = new Random(0);
        List<UserOpType> ops = Arrays.asList(new UserOpType[]{P, P, P, ZI, ZO});

        EnumeratedDistribution<CategoricalColumn> colDistribution = null;
        if (!catColPairs.isEmpty()) {
            colDistribution = new EnumeratedDistribution<>(catColPairs);
            colDistribution.reseedRandomGenerator(0);
        }

        Random randomFilterValueGen = new Random(0);
        List<Query> queries = new ArrayList<>();
        queries.add(q0);
        Query query = q0;
        for (int i = 0; i < count - 1; i++) {
            UserOpType opType = ops.get(opRand.nextInt(ops.size()));
            Rectangle rect;
            if (zoomFactor > 1 && opType.equals(ZI)) {
                rect = zoomIn(query);
            } else if (zoomFactor > 1 && opType.equals(ZO)) {
                rect = zoomOut(query);
            } else {
                rect = pan(query, shifts[i], directions[i]);
                if (opType.equals(ZI) || opType.equals(ZO)) {
                    opType = P;
                }
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
            // // generating the index using Math.random()
            // int index = (int)(Math.random() * q0.getMeasureCols().size());
            // List<Integer> measureCols2 = new ArrayList<>();
            // measureCols2.add(q0.getMeasureCols().get(index));
            query = new Query(rect, filters, q0.getGroupByCols(), schema.getMeasureCols(), opType); // Set the operation type
            queries.add(query);
        }
        return queries;
    }

    private Rectangle pan(Query query, int shift, Direction direction) {
        Range<Double> xRange = query.getRect().getXRange();
        Range<Double> yRange = query.getRect().getYRange();
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
        return zoom(query, 1.0 / zoomFactor);
    }

    private Rectangle zoom(Query query, double zoomFactor) {
        Range<Double> xRange = query.getRect().getXRange();
        Range<Double> yRange = query.getRect().getYRange();

        double xMiddle = (xRange.upperEndpoint() + xRange.lowerEndpoint()) / 2.0;
        double yMiddle = (yRange.upperEndpoint() + yRange.lowerEndpoint()) / 2.0;
        double newXSize = (xRange.upperEndpoint() - xRange.lowerEndpoint()) * zoomFactor;
        double newYSize = (yRange.upperEndpoint() - yRange.lowerEndpoint()) * zoomFactor;

        return new Rectangle(Range.open(xMiddle - (newXSize / 2.0), xMiddle + (newXSize / 2.0)),
                Range.open(yMiddle - (newYSize / 2.0), yMiddle + (newYSize / 2.0)));
    }

    private Range<Double> adjustRange(Range<Double> range, int shift) {
        double interval = (range.upperEndpoint() -
                range.lowerEndpoint()) * shift / 100;
        return Range.open(range.lowerEndpoint() + interval, range.upperEndpoint() + interval);
    }

}
