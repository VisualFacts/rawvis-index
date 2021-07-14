package gr.athenarc.imsi.visualfacts.experiments.util;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.List;

public enum Direction {
    N, E, S, W, NE, NW, SE, SW;

    public static Direction[] getRandomDirections(int count) {
        List<Pair<Direction, Double>> directionPairs = new ArrayList<>();
/*        directionPairs.add(new Pair<>(N, 1d));
        directionPairs.add(new Pair<>(E, 1d));
        directionPairs.add(new Pair<>(S, 1d));
        directionPairs.add(new Pair<>(W, 1d));
        directionPairs.add(new Pair<>(NE, 1d));
        directionPairs.add(new Pair<>(NW, 1d));
        directionPairs.add(new Pair<>(SE, 1d));
        directionPairs.add(new Pair<>(SW, 1d));*/

        //SYNTH
        directionPairs.add(new Pair<>(N, 1d));
        directionPairs.add(new Pair<>(E, 0.5d));
        directionPairs.add(new Pair<>(S, 0.5d));
        directionPairs.add(new Pair<>(W, 1d));
        directionPairs.add(new Pair<>(NE, 0.5d));
        directionPairs.add(new Pair<>(NW, 1d));
        directionPairs.add(new Pair<>(SE, 0.5d));
        directionPairs.add(new Pair<>(SW, 0.5d));

/*        //TAXI 10%shift
        directionPairs.add(new Pair<>(N, 1d));
        directionPairs.add(new Pair<>(E, 1d));
        directionPairs.add(new Pair<>(S, 0.5d));
        directionPairs.add(new Pair<>(W, 0.5d));
        directionPairs.add(new Pair<>(NE, 1d));
        directionPairs.add(new Pair<>(NW, 0.5d));
        directionPairs.add(new Pair<>(SE, 0.5d));
        directionPairs.add(new Pair<>(SW, 0.5d));*/

      /*  //NETWORK 10%shift
        directionPairs.add(new Pair<>(N, 0.5d));
        directionPairs.add(new Pair<>(E, 0.5d));
        directionPairs.add(new Pair<>(S, 0.5d));
        directionPairs.add(new Pair<>(W, 2d));
        directionPairs.add(new Pair<>(NE, 0.5d));
        directionPairs.add(new Pair<>(NW, 0.5d));
        directionPairs.add(new Pair<>(SE, 0.5d));
        directionPairs.add(new Pair<>(SW, 2d));*/




        EnumeratedDistribution<Direction> distribution = new EnumeratedDistribution<>(directionPairs);
        distribution.reseedRandomGenerator(0);
        Direction[] directions = distribution.sample(count, new Direction[count]);
        return directions;
    }
}
