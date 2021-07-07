package gr.athenarc.imsi.visualfacts.queryER.DataStructures;


import java.io.Serializable;

public class DecomposedBlock extends AbstractBlock implements Serializable {

	private static final long serialVersionUID = 7639907495476036743L;

	// A type of block that comprises blocks of minimum size in the form of 2
	// int[] for higher efficiency. Only comparisons between entities1 and entities2
	// and for the same index are allowed, i.e., entities1[i] is exclusively comparable
	// with entities2[i]. No redundant comparisons should be present.
	private final boolean cleanCleanER;

	private int[] blockIndices;
	private final long[] entities1;
	private final long[] entities2;

	public DecomposedBlock(boolean ccER, long[] entityIds1, long[] entityIds2) {
		if (entityIds1.length != entityIds2.length) {
			System.err.println("\n\nCreating imbalanced decomposed block!!!!");
			System.err.println("Entities 1\t:\t" + entityIds1.length);
			System.err.println("Entities 2\t:\t" + entityIds2.length);
		}
		cleanCleanER = ccER;
		this.entities1 = entityIds1;
		this.entities2 = entityIds2;
		blockIndices = null;
	}

	public int[] getBlockIndices() {
		return blockIndices;
	}

	public long[] getEntities1() {
		return entities1;
	}

	public long[] getEntities2() {
		return entities2;
	}

	public boolean isCleanCleanER() {
		return cleanCleanER;
	}

	@Override
	public double getNoOfComparisons() {
		return entities1.length;
	}

	@Override
	public double getTotalBlockAssignments() {
		return 2*entities1.length;
	}

	@Override
	public void setBlockIndex(int startingIndex) {
		blockIndex = startingIndex;
		blockIndices = new int[entities1.length];
		for (int i = 0; i < entities1.length; i++) {
			blockIndices[i] = startingIndex+i;
		}
	}

	@Override
	public void setUtilityMeasure() {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
}