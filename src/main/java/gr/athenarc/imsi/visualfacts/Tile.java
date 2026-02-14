package gr.athenarc.imsi.visualfacts;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.math.Stats;
import com.google.common.math.StatsAccumulator;

import gr.athenarc.imsi.visualfacts.query.Query;
import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;

public abstract class Tile {

    private static final Logger LOG = LogManager.getLogger(Tile.class);

    protected Rectangle bounds;

    protected TreeNode root;
    List<CategoricalColumn> categoricalColumns;

    /**
     * Frozen stats from a tile that has been split. These are exact aggregate
     * statistics captured before the split destroyed the root node.
     * Only set when ALL measures had complete stats (count == pointCount)
     * and the root had no categorical children.
     */
    private Stats[] frozenStats;
    private int frozenPointCount;


    public Tile(Rectangle bounds) {
        this.bounds = bounds;
    }

    public abstract Tile getLeafTile(Point point);


    public TreeNode addPoint(Point point, String[] row) {
        return getOrAddCategoricalNode(row).addPoint(point);
    }

    protected TreeNode addPoint(Point point, Stack<Short> labels) {
        return getOrAddCategoricalNode(labels).addPoint(point);
    }

    public Rectangle getBounds() {
        return bounds;
    }

    public TreeNode getRoot() {
        return root;
    }


    public abstract List getLeafTiles();

    public abstract List<Tile> getOverlappedLeafTiles(Query query);

    /**
     * Like getOverlappedLeafTiles but always recurses to actual leaf tiles,
     * ignoring the frozen-stats short-circuit. Use this after split() to
     * ensure children are visited even if the parent has frozen stats.
     */
    public abstract List<Tile> getOverlappedActualLeafTiles(Query query);

    public abstract void split();

    public abstract int getMaxDepth();

    public abstract int getLeafTileCount();

    private TreeNode getOrAddCategoricalNode(String[] row) {
        if (root == null) {
            root = new TreeNode((short) 0);
        }
        TreeNode node = root;
        if (categoricalColumns == null) {
            return node;
        }
        for (CategoricalColumn categoricalColumn : categoricalColumns) {
            TreeNode child = node.getOrAddChild(categoricalColumn.getValueKey(row[categoricalColumn.getIndex()]));
            node = child;
        }
        return node;
    }

    private TreeNode getOrAddCategoricalNode(Stack<Short> labels) {
        if (root == null) {
            root = new TreeNode((short) 0);
        }
        TreeNode node = root;
        for (short label : labels) {
            TreeNode child = node.getOrAddChild(label);
            node = child;
        }
        return node;
    }

/*    private TreeNode getCategoricalNode(Stack<Short> labels) {
        TreeNode node = root;
        if (node == null)
            return null;

        for (short label : labels) {
            TreeNode child = node.getChild(label);
            node = child;
            if (node == null)
                return null;
        }
        return node;
    }*/

    public List<QueryNode> getQueryNodes(Query query, ContainmentExaminer containmentExaminer, Schema schema) {
        //we keep the old list of attrs in case node nodes match the query in this tile so that we dont expand trees unnecessarily
        List<CategoricalColumn> oldCatAttrs = categoricalColumns;

        categoricalColumns = categoricalColumns == null || categoricalColumns.isEmpty() ? new ArrayList<>() : new ArrayList<>(categoricalColumns);
        Set<Integer> treeAttrIndexes = categoricalColumns.stream().map(CategoricalColumn::getIndex).collect(Collectors.toSet());

        //we check if the tree's categorical attributes lack any of the attrs included in the query
        Set<Integer> queryAttrs = new HashSet<>(query.getCategoricalFilters().keySet());

        if (query.getGroupByCols() != null) {
            queryAttrs.addAll(query.getGroupByCols());
        }
        List<CategoricalColumn> unknownQueryAttrs = queryAttrs.stream().filter(attr -> !treeAttrIndexes.contains(attr))
                .map(attrIndex -> schema.getCategoricalColumn(attrIndex))
                .sorted(Comparator.comparingInt(CategoricalColumn::getCardinality)).collect(Collectors.toList());

        categoricalColumns.addAll(unknownQueryAttrs);
        List<QueryNode> queryNodes = new ArrayList<>();
        if (root != null) {
            List<Short> pattern = categoricalColumns.stream().map(categoricalColumn -> {
                String filterValue = query.getCategoricalFilters().get(categoricalColumn.getIndex());
                return filterValue == null ? null : categoricalColumn.getValueKey(filterValue);
            }).collect(Collectors.toList());
            queryNodes = getQueryNodesRec(query, queryNodes, containmentExaminer, root, pattern, 0, new Short[categoricalColumns.size()], schema);
        }

        if (queryNodes.isEmpty()) {
            categoricalColumns = oldCatAttrs;
        }
        return queryNodes;
    }


    private List<QueryNode> getQueryNodesRec(Query query, List<QueryNode> list, ContainmentExaminer containmentExaminer, TreeNode node, List<Short> pattern, int level, Short[] values, Schema schema) {
        // we are at a leaf node
        if (node.getChildren() == null || node.getChildren().isEmpty()) {
            Map<Integer, Short> groupByValues = new HashMap<>();
            if (query.getGroupByCols() != null && !query.getGroupByCols().isEmpty()) {
                for (int i = 0; i < level; i++) {
                    CategoricalColumn categoricalColumn = categoricalColumns.get(i);
                    if (query.getGroupByCols().contains(categoricalColumn.getIndex())) {
                        groupByValues.put(categoricalColumn.getIndex(), values[i]);
                    }
                }
            }
            list.add(new QueryNode(node, this, containmentExaminer, groupByValues, getUnknownAttrs(level), query, schema));
            return list;
        }

        Short label = pattern.get(level);

        // no filter set for current categorical
        if (label == null) {
            for (TreeNode child : node.getChildren()) {
                values[level] = child.getLabel();
                getQueryNodesRec(query, list, containmentExaminer, child, pattern, level + 1, values, schema);
            }
        } else {
            TreeNode child = node.getChild(label);
            if (child != null) {
                values[level] = child.getLabel();
                getQueryNodesRec(query, list, containmentExaminer, child, pattern, level + 1, values, schema);
            }
        }
        return list;
    }

    private List<CategoricalColumn> getUnknownAttrs(int level) {
        return new ArrayList<>(categoricalColumns.subList(level, categoricalColumns.size()));
    }

    protected void reAddPoints(TreeNode node, Stack<Short> labels) {
        if (node.getChildren() != null) {
            for (TreeNode child : node.getChildren()) {
                labels.push(child.getLabel());
                reAddPoints(child, labels);
                labels.pop();
            }
        } else {
            for (Point point : node.getPoints()) {
                this.addPoint(point, labels);
            }
        }
    }

    public List<CategoricalColumn> getCategoricalColumns() {
        return categoricalColumns;
    }

    public void setCategoricalColumns(List<CategoricalColumn> categoricalColumns) {
        this.categoricalColumns = categoricalColumns;
    }

    /**
     * Freezes the current root node's complete stats before splitting.
     * Only freezes when:
     * - root exists with non-null points
     * - root has NO categorical children (categorical attributes formally unsupported)
     * - ALL measures have complete stats (count == point count)
     *
     * This enables short-circuiting subtree traversal for future queries
     * that fully contain this tile.
     */
    public void freezeStats() {
        if (root == null || root.getPoints() == null) return;

        // Categorical trees: root has children representing categorical attribute branches.
        // Freezing stats for categorical trees is not supported — categorical query
        // processing is currently disabled. When categorical support is re-enabled,
        // this method should be extended to aggregate stats across categorical leaves.
        if (root.getChildren() != null && !root.getChildren().isEmpty()) {
            LOG.trace("Skipping stats freeze for tile with categorical tree: {}", bounds);
            return;
        }

        StatsAccumulator[] nodeStats = root.getStatsArray();
        if (nodeStats == null || nodeStats.length == 0) return;

        int pointCount = root.getPoints().size();
        Stats[] candidate = new Stats[nodeStats.length];
        for (int i = 0; i < nodeStats.length; i++) {
            if (nodeStats[i] == null || nodeStats[i].count() != pointCount) {
                return; // Not all measures complete — don't freeze
            }
            candidate[i] = nodeStats[i].snapshot();
        }
        frozenStats = candidate;
        frozenPointCount = pointCount;
        LOG.trace("Froze exact stats for tile {} ({} points, {} measures)",
                bounds, pointCount, nodeStats.length);
    }

    /**
     * Returns true if this tile has frozen exact stats from a prior split.
     * When true, all measures are guaranteed to have complete stats.
     */
    public boolean hasFrozenStats() {
        return frozenStats != null;
    }

    /**
     * Returns the frozen Stats snapshot for the given measure index,
     * or null if not available.
     */
    public Stats getFrozenStats(int measureIndex) {
        if (frozenStats == null || measureIndex < 0 || measureIndex >= frozenStats.length) {
            return null;
        }
        return frozenStats[measureIndex];
    }

    public int getFrozenPointCount() {
        return frozenPointCount;
    }

    @Override
    public String toString() {
        return "Tile{" +
                "bounds=" + bounds +
                ", root=" + root +
                ", categoricalColumns=" + categoricalColumns +
                ", frozenStats=" + (frozenStats != null ? "yes(" + frozenPointCount + " pts)" : "no") +
                '}';
    }
}