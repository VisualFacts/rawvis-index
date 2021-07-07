package gr.athenarc.imsi.visualfacts.queryER.DataStructures;

import java.io.Serializable;


public class Comparison implements Serializable {

	private static final long serialVersionUID = 723425435776147L;

	private final boolean cleanCleanER;
	private final long entityId1;
	private final long entityId2;
	private double utilityMeasure;

	public Comparison (boolean ccER, long l, long m) {
		cleanCleanER = ccER;
		entityId1 = l;
		entityId2 = m;
		utilityMeasure = -1;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final Comparison other = (Comparison) obj;
		if (this.entityId1 != other.getEntityId1()) {
			return false;
		}
		if (this.entityId2 != other.getEntityId2()) {
			return false;
		}
		return true;
	}

	public long getEntityId1() {
		return entityId1;
	}

	public long getEntityId2() {
		return entityId2;
	}

	public double getUtilityMeasure() {
		return utilityMeasure;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 61 * hash + (int) this.entityId1;
		hash = 61 * hash + (int) this.entityId2;
		return hash;
	}

	public boolean isCleanCleanER() {
		return cleanCleanER;
	}

	public void setUtilityMeasure(double utilityMeasure) {
		this.utilityMeasure = utilityMeasure;
	}
}