package gr.athenarc.imsi.visualfacts.init;

import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.Tile;
import gr.athenarc.imsi.visualfacts.query.Query;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class OptimizedGreedyInitializationPolicy extends InitializationPolicy {
    private static final Logger LOG = LogManager.getLogger(OptimizedGreedyInitializationPolicy.class);


    public OptimizedGreedyInitializationPolicy(Query q0, int noOfSubTiles, Schema schema, Integer categoricalNodeBudget) {
        super(q0, noOfSubTiles, schema, categoricalNodeBudget);
    }


    @Override
    public void initTileTreeCategoricalAttrs(List<Tile> leafTiles) {
        if (categoricalColumns == null || categoricalColumns.size() == 0)
            return;

        List<TileTreePair> tileTreePairs = new ArrayList<>(leafTiles.size());

        double totalUtil = 0d;
        //we sort the attrs in an assigned tree by their cardinality so that attrs with smaller domain go higher in the tree
        categoricalColumns.sort(Comparator.comparingInt(CategoricalColumn::getCardinality));

        for (Tile tile : leafTiles) {
            List<CategoricalColumn> catAttrs = new ArrayList<>(categoricalColumns);
            int costEstimate = computeTileTreeCostEstimate(tile, catAttrs);
            double util = computeTileTreeUtil(tile, catAttrs);
            TileTreePair tileTreePair = new TileTreePair(tile, catAttrs, costEstimate, util);
            tileTreePairs.add(tileTreePair);
        }
        Comparator<TileTreePair> comparator = Comparator.comparingDouble(value -> value.getUtil());
        tileTreePairs.sort(comparator.reversed());

        Iterator<TileTreePair> it = tileTreePairs.iterator();

        int[] treesByAttrCount = new int[categoricalColumns.size()];
        while (catNodeBudget > 0 && it.hasNext()) {
            TileTreePair tileTreePair = it.next();
            Tile leafTile = tileTreePair.getTile();
            int costEstimate = tileTreePair.getCostEstimate();
            if (leafTile.getCategoricalColumns() == null && catNodeBudget >= costEstimate) {
                //LOG.debug("treeAttrCount: " + tileTreePair.getTreeAttrCount() + ", util: " + tileTreePair.getUtil() + ", tree cost estimate: " + costEstimate + ", distance " + q0.getRect().distanceFrom(leafTile.getBounds()));
                List<CategoricalColumn> assignedAttrs = tileTreePair.getCategoricalColumns();
                leafTile.setCategoricalColumns(assignedAttrs);
                catNodeBudget -= costEstimate;
                totalUtil += tileTreePair.getUtil();
                treesByAttrCount[assignedAttrs.size() - 1]++;
            }
        }
        LOG.debug("Initial Optimized GRD assignments: " + Arrays.toString(treesByAttrCount));
    }
}
