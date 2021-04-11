package gr.athenarc.imsi.visualfacts;

import gr.athenarc.imsi.visualfacts.util.ContainmentExaminer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class QueryNode implements Iterable<Point> {

    private Map<Integer, Short> groupByValues;
    private TreeNode node;
    private Tile tile;
    private ContainmentExaminer containmentExaminer;
    private List<CategoricalColumn> unknownCatAttrs;


    public QueryNode(TreeNode node, Tile tile, ContainmentExaminer containmentExaminer,  Map<Integer, Short> groupByValues, List<CategoricalColumn> unknownCatAttrs) {
        this.groupByValues = groupByValues;
        this.node = node;
        this.tile = tile;
        this.containmentExaminer = containmentExaminer;
        this.unknownCatAttrs = unknownCatAttrs;
    }

    public  Map<Integer, Short> getGroupByValues() {
        return groupByValues;
    }

    public TreeNode getNode() {
        return node;
    }

    public Tile getTile() {
        return tile;
    }

    public ContainmentExaminer getContainmentExaminer() {
        return containmentExaminer;
    }

    public List<CategoricalColumn> getUnknownCatAttrs() {
        return unknownCatAttrs;
    }

    public boolean isFullyContained() {
        return containmentExaminer == null;
    }

    @Override
    public Iterator<Point> iterator() {
        return new NodePointsIterator(this);
    }

    @Override
    public String toString() {
        return "QueryNode{" +
                "groupByValues=" + groupByValues +
                ", node=" + node +
                ", tile=" + tile +
                ", containmentExaminer=" + containmentExaminer +
                ", unknownCatAttrs=" + unknownCatAttrs +
                '}';
    }
}