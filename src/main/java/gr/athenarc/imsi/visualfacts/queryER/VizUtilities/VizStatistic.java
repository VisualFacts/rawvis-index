package gr.athenarc.imsi.visualfacts.queryER.VizUtilities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class VizStatistic implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8129873355065905104L;
	
	public double percentOfDups;
	public HashMap<Integer, Double> similarityMeasures;
	public LinkedHashMap<Integer, HashMap<String, Integer>> columnValues;
	
	public VizStatistic(double percentOfDups, HashMap<Integer, Double> avgColumSimilarities, LinkedHashMap<Integer, HashMap<String, Integer>> clustersColumnValues) {
		super();
		this.percentOfDups = percentOfDups;
		this.similarityMeasures = avgColumSimilarities;
		this.columnValues = clustersColumnValues;
	}
}
