package gr.athenarc.imsi.visualfacts;
import java.util.Stack;

public interface TreeNodeVisitor {

    public void visit(TreeNode treeNode, Stack<Short> values);

}
