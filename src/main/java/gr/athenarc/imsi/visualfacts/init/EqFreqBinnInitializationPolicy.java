package gr.athenarc.imsi.visualfacts.init;

import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.Tile;
import gr.athenarc.imsi.visualfacts.query.Query;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class EqFreqBinnInitializationPolicy extends InitializationPolicy {
    private static final Logger LOG = LogManager.getLogger(EqFreqBinnInitializationPolicy.class);

    private int binCount;


    public EqFreqBinnInitializationPolicy(Query q0, int noOfSubTiles, Schema schema, Integer categoricalNodeBudget, int binCount) {
        super(q0, noOfSubTiles, schema, categoricalNodeBudget);
        this.binCount = binCount;
    }


    @Override
    public double initTileTreeCategoricalAttrs(List<Tile> leafTiles) {
        if (categoricalColumns == null || categoricalColumns.size() == 0)
            return -1d;

        double totalUtil = 0d;

        Comparator<Tile> comparator = Comparator.comparingDouble(tile -> computeTileProbPerSurfaceArea(tile));
        leafTiles.sort(comparator.reversed());
        Iterable<List<Tile>> bins = Iterables.partition(leafTiles, leafTiles.size() / binCount);


        int[] treesByAttrCount = new int[categoricalColumns.size()];

        Iterator<List<Tile>> binIterator = bins.iterator();
        while (catNodeBudget > 0 && binIterator.hasNext()) {
            List<Tile> binTiles = binIterator.next();
            for (int i = categoricalColumns.size(); i >= 1; i--) {
                List<CategoricalColumn> catAttrs = new ArrayList<>(categoricalColumns.subList(0, i));
                //we sort the attrs in an assigned tree by their cardinality so that attrs with smaller domain go higher in the tree
                catAttrs.sort(Comparator.comparingInt(CategoricalColumn::getCardinality));

                int costEstimate = binTiles.stream().mapToInt(tile -> computeTileTreeCostEstimate(tile, catAttrs)).sum();
                if (catNodeBudget - costEstimate >= 0) {
                    for (Tile tile : binTiles) {
                        tile.setCategoricalColumns(catAttrs);
                        totalUtil += computeTileTreeUtil(tile, catAttrs);
                        treesByAttrCount[catAttrs.size() - 1]++;
                    }
                    catNodeBudget -= costEstimate;
                    break;
                }
            }
        }
        LOG.debug("Initial BINN assignments: " + Arrays.toString(treesByAttrCount));
        LOG.debug("Total Index Util: " + totalUtil);
        return totalUtil;
    }
}
