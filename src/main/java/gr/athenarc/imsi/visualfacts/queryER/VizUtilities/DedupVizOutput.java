package gr.athenarc.imsi.visualfacts.queryER.VizUtilities;

import java.io.Serializable;
import java.util.List;

public class DedupVizOutput implements Serializable {

	private static final long serialVersionUID = 6208168040968847318L;

	public VizStatistic VizStatistic;
	public List<VizCluster> VizDataset;
	
	public DedupVizOutput(List<VizCluster> VizDataset, VizStatistic VizStatistic) {
		super();
		this.VizDataset = VizDataset;
		this.VizStatistic = VizStatistic;
	}
	
	
}
