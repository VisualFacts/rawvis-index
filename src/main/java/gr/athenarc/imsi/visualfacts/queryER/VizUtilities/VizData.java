package gr.athenarc.imsi.visualfacts.queryER.VizUtilities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class VizData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 464784220057275840L;
	public Long offset;
	public HashMap<Integer, String> columns;		
	
	public VizData(long idInner, HashMap<Integer, String> columns) {
		this.offset = idInner;
		this.columns = columns;
	}
	
}
