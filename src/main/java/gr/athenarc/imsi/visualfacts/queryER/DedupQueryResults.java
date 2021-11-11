package gr.athenarc.imsi.visualfacts.queryER;

import java.util.HashMap;
import java.util.Set;

import gr.athenarc.imsi.visualfacts.queryER.DataStructures.EntityResolvedTuple;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.EntityGrouping;
import gr.athenarc.imsi.visualfacts.queryER.VizUtilities.DedupVizOutput;

public class DedupQueryResults {

	private EntityResolvedTuple entityResolvedTuple;
	
	private DedupVizOutput dedupVizOutput;
	
	public DedupQueryResults(EntityResolvedTuple entityResolvedTuple) {
		// TODO Auto-generated constructor stub
		this.entityResolvedTuple = entityResolvedTuple;
	}

	public void groupSimilar(){
		
		HashMap<Long, Set<Long>> revUF = entityResolvedTuple.getRevUF();
		HashMap<Long, Object[]> data = entityResolvedTuple.getData();
		HashMap<Long, HashMap<Long,Double>>  similarities = entityResolvedTuple.getSimilarities();
		int datasourceColumn = entityResolvedTuple.getDatasourceColumn();
		dedupVizOutput =  EntityGrouping.groupSimilar(revUF, data, similarities, datasourceColumn);
	}
	
	public Integer getComparisons() {
		// TODO Auto-generated method stub
		return entityResolvedTuple.getComparisons();
	}

	public DedupVizOutput getDedupVizOutput() {
		return dedupVizOutput;
	}
	
}
