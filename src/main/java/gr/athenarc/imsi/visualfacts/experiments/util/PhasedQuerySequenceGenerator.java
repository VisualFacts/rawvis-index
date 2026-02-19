package gr.athenarc.imsi.visualfacts.experiments.util;

import static gr.athenarc.imsi.visualfacts.experiments.util.UserOpType.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Range;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.experiments.config.PhaseConfig;
import gr.athenarc.imsi.visualfacts.query.Query;

/**
 * Generates a query sequence from a list of phases, supporting pan, zoom_to_point, and zoom_out operations.
 * 
 * <p>This implements multi-phase exploration scenarios where each phase has its own
 * operation type and parameters, enabling reproducible benchmarks like
 * "overview first, zoom in, explore details, zoom out."
 * 
 * <p>Operations:
 * <ul>
 *   <li><b>pan</b>: viewport shifts in a direction sampled from configurable directionWeights,
 *       size stays constant. Optionally clamped to a bounding box.</li>
 *   <li><b>zoom_to_point</b>: cursor-anchored zoom-in. Each edge is scaled toward a target point
 *       by the given zoomFactor (0.5 = 2× zoom-in). This is how Google Maps / Leaflet zooms work.</li>
 *   <li><b>zoom_out</b>: center-anchored zoom-out. Each edge is scaled away from the viewport center
 *       by the given zoomFactor (2.0 = 2× zoom-out).</li>
 * </ul>
 * 
 * <p>All random operations are seeded for reproducibility.
 */
public class PhasedQuerySequenceGenerator {

    private static final Logger LOG = LogManager.getLogger(PhasedQuerySequenceGenerator.class);
    private static final long SEED = 0L;

    private final List<PhaseConfig> phases;

    public PhasedQuerySequenceGenerator(List<PhaseConfig> phases) {
        this.phases = phases;
    }

    /**
     * Generates the full query sequence by concatenating all phases.
     * The first query is always q0, and each phase continues from the last query of the previous phase.
     */
    public List<Query> generateQuerySequence(Query q0, Schema schema) {
        List<Query> queries = new ArrayList<>();
        queries.add(q0);

        Query current = q0;
        Random shiftRand = new Random(SEED);

        for (PhaseConfig phase : phases) {
            LOG.info("Generating phase '{}': {} {} queries", phase.getName(), phase.getOperation(), phase.getCount());

            // Pre-generate directions and shifts for the entire phase (seeded for reproducibility)
            Direction[] directions = null;
            if ("pan".equals(phase.getOperation())) {
                directions = Direction.getRandomDirections(phase.getCount(), phase.getDirectionWeights());
            }

            for (int i = 0; i < phase.getCount(); i++) {
                Query next = generateNextQuery(current, phase, directions, i, shiftRand, schema);
                queries.add(next);
                current = next;
            }
        }

        LOG.info("Generated {} total queries across {} phases", queries.size(), phases.size());
        return queries;
    }

    private Query generateNextQuery(Query current, PhaseConfig phase, Direction[] directions,
                                     int phaseIndex, Random shiftRand, Schema schema) {
        Rectangle rect;
        UserOpType opType;

        switch (phase.getOperation()) {
            case "pan":
                rect = pan(current.getRect(), phase, directions[phaseIndex], shiftRand);
                opType = P;
                break;
            case "zoom_to_point":
                rect = zoomToPoint(current.getRect(), phase);
                opType = ZI;
                break;
            case "zoom_out":
                rect = zoomOut(current.getRect(), phase);
                opType = ZO;
                break;
            default:
                throw new IllegalArgumentException("Unknown phase operation: " + phase.getOperation());
        }

        return new Query(rect, current.getCategoricalFilters(), current.getGroupByCols(),
                schema.getMeasureCols(), opType);
    }

    /**
     * Pan: shift the viewport in a pre-sampled direction.
     * Shift amount is a percentage of viewport width/height (same as QuerySequenceGenerator).
     * If clampToBounds is set, the result is clamped.
     */
    private Rectangle pan(Rectangle current, PhaseConfig phase, Direction direction, Random shiftRand) {
        int shift = shiftRand.nextInt(phase.getMaxShift() - phase.getMinShift() + 1) + phase.getMinShift();

        Range<Double> xRange = current.getXRange();
        Range<Double> yRange = current.getYRange();

        switch (direction) {
            case N: case NE: case NW:
                yRange = adjustRange(yRange, shift);
                break;
            case S: case SE: case SW:
                yRange = adjustRange(yRange, -shift);
                break;
            default:
                break;
        }
        switch (direction) {
            case E: case NE: case SE:
                xRange = adjustRange(xRange, shift);
                break;
            case W: case NW: case SW:
                xRange = adjustRange(xRange, -shift);
                break;
            default:
                break;
        }

        Rectangle result = new Rectangle(xRange, yRange);

        // Clamp if bounds are specified
        if (phase.getClampToBounds() != null && !phase.getClampToBounds().isEmpty()) {
            result = clamp(result, phase.getClampToBounds());
        }

        return result;
    }

    /**
     * Zoom toward a target point (cursor-anchored zoom).
     * Each edge is scaled toward the target point by zoomFactor.
     * With zoomFactor=0.5, the viewport halves in size and its center drifts toward the target.
     * 
     * Formula: edge' = target + zoomFactor * (edge - target)
     */
    private Rectangle zoomToPoint(Rectangle current, PhaseConfig phase) {
        double[] target = phase.getTargetPoint();
        if (target == null) {
            throw new IllegalArgumentException("zoom_to_point phase requires a 'target' parameter (format: 'x,y')");
        }

        double px = target[0];
        double py = target[1];
        double f = phase.getZoomFactor();

        double newXLow = px + f * (current.getXRange().lowerEndpoint() - px);
        double newXHigh = px + f * (current.getXRange().upperEndpoint() - px);
        double newYLow = py + f * (current.getYRange().lowerEndpoint() - py);
        double newYHigh = py + f * (current.getYRange().upperEndpoint() - py);

        return new Rectangle(Range.open(newXLow, newXHigh), Range.open(newYLow, newYHigh));
    }

    /**
     * Zoom out from the viewport center.
     * Each edge is scaled away from the center by zoomFactor.
     * With zoomFactor=2.0, the viewport doubles in size (center stays the same).
     */
    private Rectangle zoomOut(Rectangle current, PhaseConfig phase) {
        double f = phase.getZoomFactor();

        double xMid = (current.getXRange().lowerEndpoint() + current.getXRange().upperEndpoint()) / 2.0;
        double yMid = (current.getYRange().lowerEndpoint() + current.getYRange().upperEndpoint()) / 2.0;

        double halfXSize = (current.getXRange().upperEndpoint() - current.getXRange().lowerEndpoint()) / 2.0 * f;
        double halfYSize = (current.getYRange().upperEndpoint() - current.getYRange().lowerEndpoint()) / 2.0 * f;

        return new Rectangle(Range.open(xMid - halfXSize, xMid + halfXSize),
                Range.open(yMid - halfYSize, yMid + halfYSize));
    }

    /**
     * Adjusts a range by shifting it by a percentage of its span.
     * Shift is in percent: shift=10 means move by 10% of the range width.
     */
    private Range<Double> adjustRange(Range<Double> range, int shift) {
        double interval = (range.upperEndpoint() - range.lowerEndpoint()) * shift / 100.0;
        return Range.open(range.lowerEndpoint() + interval, range.upperEndpoint() + interval);
    }

    /**
     * Clamps a rectangle to stay within the specified bounds.
     * If the viewport exceeds a bound, it is shifted inward (preserving its size).
     */
    private Rectangle clamp(Rectangle rect, String boundsStr) {
        Rectangle bounds = QueryUtils.convertToRectangle(boundsStr);

        double xLow = rect.getXRange().lowerEndpoint();
        double xHigh = rect.getXRange().upperEndpoint();
        double yLow = rect.getYRange().lowerEndpoint();
        double yHigh = rect.getYRange().upperEndpoint();

        double width = xHigh - xLow;
        double height = yHigh - yLow;

        // Clamp X: shift the whole viewport if it exceeds bounds
        if (xLow < bounds.getXRange().lowerEndpoint()) {
            xLow = bounds.getXRange().lowerEndpoint();
            xHigh = xLow + width;
        }
        if (xHigh > bounds.getXRange().upperEndpoint()) {
            xHigh = bounds.getXRange().upperEndpoint();
            xLow = xHigh - width;
        }

        // Clamp Y
        if (yLow < bounds.getYRange().lowerEndpoint()) {
            yLow = bounds.getYRange().lowerEndpoint();
            yHigh = yLow + height;
        }
        if (yHigh > bounds.getYRange().upperEndpoint()) {
            yHigh = bounds.getYRange().upperEndpoint();
            yLow = yHigh - height;
        }

        return new Rectangle(Range.open(xLow, xHigh), Range.open(yLow, yHigh));
    }
}
