package gr.athenarc.imsi.visualfacts.init;

import gr.athenarc.imsi.visualfacts.Rectangle;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.query.Query;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Determines the initial spatial grid layout based on query probability.
 * Uses a normal distribution centered on q0 to allocate finer sub-tiles
 * in high-probability regions.
 */
public class InitializationPolicy {

    private static final Logger LOG = LogManager.getLogger(InitializationPolicy.class);

    private final NormalDistribution distributionX;
    private final NormalDistribution distributionY;
    private final int noOfSubtiles;

    public InitializationPolicy(Query q0, int noOfSubTiles, Schema schema) {
        Rectangle rect = q0.getRect();
        this.distributionX = new NormalDistribution(rect.getCenterX(), rect.getXSize());
        this.distributionY = new NormalDistribution(rect.getCenterY(), rect.getYSize());
        this.noOfSubtiles = noOfSubTiles;
    }

    public double computeRectProb(Rectangle rect) {
        return distributionX.probability(rect.getXRange().lowerEndpoint(), rect.getXRange().upperEndpoint()) *
                distributionY.probability(rect.getYRange().lowerEndpoint(), rect.getYRange().upperEndpoint());
    }

    public int computeSplitSize(Rectangle rect) {
        return (int) Math.floor(Math.sqrt(noOfSubtiles * this.computeRectProb(rect)));
    }
}
