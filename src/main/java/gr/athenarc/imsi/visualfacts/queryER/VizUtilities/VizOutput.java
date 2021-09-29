package gr.athenarc.imsi.visualfacts.queryER.VizUtilities;

import java.io.Serializable;
import java.util.List;

public class VizOutput implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6208168040968847318L;

	public double percentOfDups = 0.0;
	public VizStatistic VizStatistic;
	public List<VizCluster> VizDataset;
	
	public VizOutput(List<VizCluster> VizDataset, VizStatistic VizStatistic) {
		super();
		this.VizDataset = VizDataset;
		this.VizStatistic = VizStatistic;
	}
	
	
	
}
