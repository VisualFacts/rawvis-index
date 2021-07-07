package gr.athenarc.imsi.visualfacts.queryER.DataStructures;

import java.io.Serializable;
import java.util.Arrays;

public class BilateralBlock extends AbstractBlock implements Serializable {

	private static final long serialVersionUID = 75264711552351524L;

	private final long[] index1Entities;
	private final long[] index2Entities;

	public BilateralBlock(long[] blockEntitiesD1, long[] blockEntitiesD2) {
		super();
		index1Entities = blockEntitiesD1;
		index2Entities = blockEntitiesD2;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final BilateralBlock other = (BilateralBlock) obj;
		if (!Arrays.equals(this.index1Entities, other.index1Entities)) {
			return false;
		}
		if (!Arrays.equals(this.index2Entities, other.index2Entities)) {
			return false;
		}
		return true;
	}

	public long[] getIndex1Entities() {
		return index1Entities;
	}

	public long[] getIndex2Entities() {
		return index2Entities;
	}

	@Override
	public double getNoOfComparisons() {
		return ((double) index1Entities.length) * ((double) index2Entities.length);
	}

	@Override
	public double getTotalBlockAssignments() {
		return index1Entities.length+index2Entities.length;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 41 * hash + Arrays.hashCode(this.index1Entities);
		hash = 41 * hash + Arrays.hashCode(this.index2Entities);
		return hash;
	}

	@Override
	public void setUtilityMeasure() {
		utilityMeasure = 1.0/Math.max(index1Entities.length, index2Entities.length);
	}
}