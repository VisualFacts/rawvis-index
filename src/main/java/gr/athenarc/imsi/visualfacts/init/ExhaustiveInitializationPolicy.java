package gr.athenarc.imsi.visualfacts.init;

import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.Tile;
import gr.athenarc.imsi.visualfacts.query.Query;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ExhaustiveInitializationPolicy extends InitializationPolicy {
    private static final Logger LOG = LogManager.getLogger(ExhaustiveInitializationPolicy.class);

    private int[] assignments;

    public ExhaustiveInitializationPolicy(Query q0, int noOfSubTiles, Schema schema, Integer categoricalNodeBudget) {
        super(q0, noOfSubTiles, schema, categoricalNodeBudget);
    }

    public void initTileTreeCategoricalAttrs(List<Tile> leafTiles) {
        if (categoricalColumns == null || categoricalColumns.size() == 0)
            return;

        Comparator<Tile> comparator = Comparator.comparingDouble(tile -> this.computeTileProbPerSurfaceArea(tile));
        leafTiles.sort(comparator.reversed());
        LOG.debug(catNodeBudget);

        assignments = new int[leafTiles.size()];
        // LOG.debug("Initial NAIVE INIT assignments: " + treeCount);
        initTileTreeCategoricalAttrs(leafTiles, 0, catNodeBudget);
    }

    private double initTileTreeCategoricalAttrs(List<Tile> leafTiles, int startTileIndex, int budget) {
        double maxUtil = 0;
        if (startTileIndex == leafTiles.size()){
            return 0;
        }
        Tile currentTile = leafTiles.get(startTileIndex);
        for (int i = 1; i <= categoricalColumns.size(); i++) {
            List<CategoricalColumn> catAttrs = new ArrayList<>(categoricalColumns.subList(0, i));
            int costEstimate = computeTileTreeCostEstimate(currentTile, catAttrs);
            int newBudget = budget - costEstimate;

            if (newBudget < 0) {
                continue;
            }
            double util = computeTileTreeUtil(currentTile, catAttrs);
            maxUtil = Math.max(maxUtil, util + initTileTreeCategoricalAttrs(leafTiles, startTileIndex + 1, newBudget));
        }

        return maxUtil;
    }
}
