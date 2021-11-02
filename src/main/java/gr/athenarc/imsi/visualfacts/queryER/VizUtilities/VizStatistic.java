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
	public VizStatistic(double percentOfDups) {
		super();
		this.percentOfDups = percentOfDups;
	}
}
