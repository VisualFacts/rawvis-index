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
	public HashMap<Integer, HashMap<String, Integer>> clusterColumns;
	public Map<Integer, HashMap<Integer, Double>> clusterSimilarities;
	public VizCluster(List<VizData> VizData, Object[] groupedObj) {
		this.VizData = VizData;
		this.groupedObj = groupedObj;
	}

	public VizCluster(List<VizData> VizData, 
			LinkedHashMap<Integer, HashMap<String, Integer>> clusterColumnValues,
			 Object[] groupedObj) {
		this.VizData = VizData;
		this.clusterColumns = clusterColumnValues;
		this.groupedObj = groupedObj;
	}

}
