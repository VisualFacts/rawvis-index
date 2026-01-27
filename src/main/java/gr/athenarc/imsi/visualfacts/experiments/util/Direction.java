package gr.athenarc.imsi.visualfacts.experiments.util;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public enum Direction {
    N, E, S, W, NE, NW, SE, SW;

    /**
     * Default weights used when no configuration is provided.
     * Uniform distribution across all directions.
     */
    private static final double DEFAULT_WEIGHT = 1.0;

    /**
     * Generates random directions using provided weights.
     * 
     * @param count number of directions to generate
     * @param weights map of direction name to weight (e.g., {"N": 1.0, "NE": 1.5, ...})
     *                If null or empty, uses uniform weights.
     * @return array of randomly sampled directions
     */
    public static Direction[] getRandomDirections(int count, Map<String, Double> weights) {
        List<Pair<Direction, Double>> directionPairs = new ArrayList<>();
        
        for (Direction dir : Direction.values()) {
            double weight = DEFAULT_WEIGHT;
            if (weights != null && weights.containsKey(dir.name())) {
                weight = weights.get(dir.name());
            }
            directionPairs.add(new Pair<>(dir, weight));
        }

        EnumeratedDistribution<Direction> distribution = new EnumeratedDistribution<>(directionPairs);
        distribution.reseedRandomGenerator(0);
        Direction[] directions = distribution.sample(count, new Direction[count]);
        return directions;
    }

    /**
     * Generates random directions using uniform weights (backward compatibility).
     */
    public static Direction[] getRandomDirections(int count) {
        return getRandomDirections(count, null);
    }
}
