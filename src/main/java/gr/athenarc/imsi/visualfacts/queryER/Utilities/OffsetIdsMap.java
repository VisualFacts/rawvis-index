package gr.athenarc.imsi.visualfacts.queryER.Utilities;

import java.util.HashMap;

public class OffsetIdsMap {

	public HashMap<Long, Integer> offsetToId;
	public HashMap<Integer, Long> idToOffset;
	public OffsetIdsMap(HashMap<Long, Integer> offsetToId, HashMap<Integer, Long> idToOffset) {
		super();
		this.offsetToId = offsetToId;
		this.idToOffset = idToOffset;
	}
	
	
	
}
