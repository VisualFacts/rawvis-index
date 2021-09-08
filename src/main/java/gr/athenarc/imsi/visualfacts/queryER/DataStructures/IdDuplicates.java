package gr.athenarc.imsi.visualfacts.queryER.DataStructures;

import java.io.Serializable;

public class IdDuplicates implements Serializable {

	private static final long serialVersionUID = 7234234586147L;

	private final long entityId1;
	private final long entityId2;

	public IdDuplicates(long l, long m) {
		entityId1 = l;
		entityId2 = m;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) entityId1;
		result = prime * result + (int) entityId2;
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IdDuplicates other = (IdDuplicates) obj;
		if (entityId1 != other.entityId1)
			return false;
		if (entityId2 != other.entityId2)
			return false;
		return true;
	}


	public long getEntityId1() {
		return entityId1;
	}

	public long getEntityId2() {
		return entityId2;
	}


	@Override
	public String toString() {
		return "IdDuplicates [entityId1=" + entityId1 + ", entityId2=" + entityId2 + "]";
	}
	
	
}