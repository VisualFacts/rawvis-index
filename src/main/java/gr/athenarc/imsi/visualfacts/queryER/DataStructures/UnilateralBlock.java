package gr.athenarc.imsi.visualfacts.queryER.DataStructures;

import java.io.Serializable;
import java.util.Arrays;

public class UnilateralBlock extends AbstractBlock implements Serializable {

	private static final long serialVersionUID = 43532585408538695L;

	protected final long[] entities;

	public UnilateralBlock(long[] idsArray) {
		super();
		this.entities = idsArray;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final UnilateralBlock other = (UnilateralBlock) obj;
		if (!Arrays.equals(this.entities, other.entities)) {
			return false;
		}
		return true;
	}

	public long[] getEntities() {
		return entities;
	}

	@Override
	public double getNoOfComparisons() {
		double noOfEntities = entities.length;
		return noOfEntities*(noOfEntities-1)/2;
	}

	@Override
	public double getTotalBlockAssignments() {
		return entities.length;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 53 * hash + Arrays.hashCode(this.entities);
		return hash;
	}

	@Override
	public void setUtilityMeasure() {
		utilityMeasure = 1.0/entities.length;
	}
}