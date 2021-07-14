package gr.athenarc.imsi.visualfacts.queryER.BigVizUtilities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class BigVizData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 464784220057275840L;
	public Long offset;
	public HashMap<String, String> columns;		
	
	public BigVizData(long idInner, HashMap<String, String> columns) {
		this.offset = idInner;
		this.columns = columns;
	}
	
}
