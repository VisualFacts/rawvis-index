package gr.athenarc.imsi.visualfacts.init;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.Tile;
import gr.athenarc.imsi.visualfacts.query.Query;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class EqRangeBinnInitPolicy extends InitializationPolicy {
    private static final Logger LOG = LogManager.getLogger(EqRangeBinnInitPolicy.class);

    private int binCount;


    public EqRangeBinnInitPolicy(Query q0, int noOfSubTiles, Schema schema, Integer categoricalNodeBudget, int binCount) {
        super(q0, noOfSubTiles, schema, categoricalNodeBudget);
        this.binCount = binCount;
    }


    @Override
    public double initTileTreeCategoricalAttrs(List<Tile> leafTiles) {
        if (categoricalColumns == null || categoricalColumns.size() == 0)
            return -1d;

        double totalUtil = 0d;

        DoubleSummaryStatistics probStats = leafTiles.stream().collect(Collectors.summarizingDouble(tile -> computeTileProbPerSurfaceArea(tile)));

        Range<Double>[] binRanges = new Range[binCount];
        double upper = probStats.getMin();
        double binRangeSize = (probStats.getMax() - probStats.getMin()) / binCount;
        for (int i = 0; i < binCount; i++) {
            Range<Double> binRange;
            if (i == binCount - 1) {
                binRange = Range.closed(upper, probStats.getMax());
            } else {
                binRange = Range.closedOpen(upper, (upper += binRangeSize));
            }
            binRanges[i] = binRange;
        }

        RangeMap<Double, List<Tile>> bins = TreeRangeMap.create();
        for (Range<Double> binRange : binRanges) {
            bins.put(binRange, new ArrayList<>());
        }

        for (Tile tile : leafTiles) {
            bins.get(computeTileProbPerSurfaceArea(tile)).add(tile);
        }

        int[] treesByAttrCount = new int[categoricalColumns.size()];

/*        bins.asDescendingMapOfRanges().entrySet().stream().forEach(e -> {
            System.out.println("Bin range: " + e.getKey() + ", Bin tile count: " + e.getValue().size());
        });*/

        Iterator<Map.Entry<Range<Double>, List<Tile>>> binIterator = bins.asDescendingMapOfRanges().entrySet().iterator();
        while (catNodeBudget > 0 && binIterator.hasNext()) {
            List<Tile> binTiles = binIterator.next().getValue();
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
        return totalUtil;
    }
}
