package gr.athenarc.imsi.visualfacts.queryER.DataStructures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import gr.athenarc.imsi.visualfacts.queryER.Utilities.EntityGrouping;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.MapUtilities;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.UnionFind;

public class EntityResolvedTuple<T> {

	public HashMap<Long, Object[]> data;

	public UnionFind uFind;
	public HashMap<Long, Set<Long>> revUF; // these is the query links
	public HashMap<Long, Set<Long>> links; // these are the total links
	public HashMap<Long, HashMap<Long,Double>> similarities;
	public List<T> finalData;
	private int matches;
	private Integer comparisons;
	private double compTime;
	private double revUFCreationTime;
	private int datasourceColumn;
	
	public EntityResolvedTuple(HashMap<Long, Object[]> data, UnionFind uFind) {
		super();
		this.data = data;
		this.uFind = uFind;
		this.finalData = new ArrayList<>();
		this.revUF = new HashMap<>();
	}
	
	
	public EntityResolvedTuple(List<Object[]> finalData, UnionFind uFind) {
		super();
		this.finalData = (List<T>) finalData;
		this.revUF = new HashMap<>();
	}
	
	@SuppressWarnings("unchecked")
	public void getAll() {
		double revUFCreationStartTime = System.currentTimeMillis();
		for (long child : uFind.getParent().keySet()) {
			long parent = uFind.getParent().get(child);
			this.revUF.computeIfAbsent(parent, x -> new HashSet<>()).add(child);
			this.revUF.computeIfAbsent(child, x -> new HashSet<>()).add(parent);
			// For both of these go to their similarities and recompute them
			for(long simPar : this.revUF.get(parent)) {
				if(simPar != parent)
					this.revUF.computeIfAbsent(simPar, x -> new HashSet<>()).addAll(this.revUF.get(parent));
			}
			for(long simPar : this.revUF.get(child)) {
				if(simPar != child)
					this.revUF.computeIfAbsent(simPar, x -> new HashSet<>()).addAll(this.revUF.get(child));
			}
		}

		double revUFCreationEndTime = System.currentTimeMillis();
		this.setRevUFCreationTime((revUFCreationEndTime - revUFCreationStartTime)/1000);
	}
	
	public HashMap<Long, Set<Long>> mergeLinks(HashMap<Long, Set<Long>> links, boolean firstDedup,
			Set<Long> totalIds) {
		this.links = links;
		if(!firstDedup) this.combineLinks(links);
		filterData(totalIds);
		return links;
	}
		
	
	public void filterData(Set<Long> totalIds) {
		HashMap<Long, Object[]> filteredData = new HashMap<>();
		// First filter the merged revUF by keeping only the query ids + dup ids
		this.revUF.keySet().retainAll(totalIds);
		this.revUF.values().forEach(v -> v.retainAll(totalIds));
		for (long id : this.revUF.keySet()) {
			Object[] datum = this.data.get(id);
			filteredData.put(id, datum);
			this.finalData.add((T) datum);
		}
		this.data = filteredData;
	}
	
	public void combineLinks(Map<Long, Set<Long>> links) {
		if(revUF != null) {

			for (Entry<Long, Set<Long>> e : revUF.entrySet()) {
				e.getValue().remove(e.getKey());
			    links.merge(e.getKey(), e.getValue(), (v1, v2) -> {
			    	v1.addAll(v2);
			    	return v1;
			    });
			}
		}
		this.revUF = (HashMap<Long, Set<Long>>) MapUtilities.deepCopy(this.links);
	}

	public int getMatches() {
		return matches;
	}

	public void setMatches(int i) {
		this.matches = i;
	}

	public Integer getComparisons() {
		return comparisons;
	}

	public void setComparisons(Integer comparisons) {
		this.comparisons = comparisons;
	}

	public HashMap<Long, Object[]> getData() {
		return data;
	}

	public void setData(HashMap<Long, Object[]> data) {
		this.data = data;
	}

	public HashMap<Long, Set<Long>> getRevUF() {
		return revUF;
	}

	public void setRevUF(HashMap<Long, Set<Long>> revUF) {
		this.revUF = revUF;
	}

	public List<T> getFinalData() {
		return finalData;
	}

	public void setFinalData(List<T> finalData) {
		this.finalData = finalData;
	}

	public double getCompTime() {
		return compTime;
	}

	public void setCompTime(double compTime) {
		this.compTime = compTime;
	}

	public double getRevUFCreationTime() {
		return revUFCreationTime;
	}

	public void setRevUFCreationTime(double revUFCreationTime) {
		this.revUFCreationTime = revUFCreationTime;
	}

	public void setDatasourceColumn(int datasourceColumn) {
		this.datasourceColumn = datasourceColumn;
	}

	public int getDatasourceColumn() {
		return datasourceColumn;
	}

	public HashMap<Long, Set<Long>> getLinks() {
		return links;
	}


	public HashMap<Long, HashMap<Long,Double>>  getSimilarities() {
		// TODO Auto-generated method stub
		return similarities;
	}



	

}
