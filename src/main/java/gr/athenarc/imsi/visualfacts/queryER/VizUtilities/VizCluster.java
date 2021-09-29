package gr.athenarc.imsi.visualfacts.queryER.VizUtilities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class VizCluster implements Serializable  {



	private static final long serialVersionUID = -7740237215045582966L;
	public List<VizData> VizData;
	public Object[] groupedObj;
	public HashMap<Integer, Double> clusterColumnSimilarity;
	public HashMap<Integer, HashMap<String, Integer>> clusterColumns;
	public Map<Integer, HashMap<Integer, Double>> clusterSimilarities;
	public VizCluster(List<VizData> VizData, Object[] groupedObj, HashMap<Integer, Double> clusterColumnSimilarity) {
		this.VizData = VizData;
		this.groupedObj = groupedObj;
		this.clusterColumnSimilarity = clusterColumnSimilarity;
	}

	public VizCluster(List<VizData> VizData, 
			HashMap<Integer, Double> clusterColumnSimilarity,
			LinkedHashMap<Integer, HashMap<String, Integer>> clusterColumns2,
			Map<Integer, HashMap<Integer, Double>> clusterSimilarities, Object[] groupedObj) {
		this.VizData = VizData;
		this.clusterColumnSimilarity = clusterColumnSimilarity;	
		this.clusterColumns = clusterColumns2;
		this.clusterSimilarities = clusterSimilarities;
		this.groupedObj = groupedObj;
	}

}
