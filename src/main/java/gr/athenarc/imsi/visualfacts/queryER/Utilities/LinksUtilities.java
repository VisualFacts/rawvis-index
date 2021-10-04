package gr.athenarc.imsi.visualfacts.queryER.Utilities;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import gr.athenarc.imsi.visualfacts.queryER.DeduplicationExecution;
import gr.athenarc.imsi.visualfacts.util.RawFileService;

public class LinksUtilities {
	
	private HashMap<Long, String[]> dataWithLinks;
	private HashMap<Long, String[]> dataWithoutLinks;
	private HashMap<Long, Set<Long>> links;
	private Set<Long> totalIds;
	private Set<Long> qIds;
	private Set<Long> qIdsNoLinks;
	private RawFileService rawFileService;
	private boolean firstDedup;
	
	public LinksUtilities(HashMap<Long, Set<Long>> links, Set<Long> qIds, RawFileService rawFileService) {
		this.links = links;
		if(links == null) firstDedup = true;
		this.qIds = qIds;       
		this.rawFileService = rawFileService;
	}

	public HashMap<Long, String[]> getDataWithLinks() {
		return dataWithLinks;
	}

	public HashMap<Long, String[]> getDataWithoutLinks() {
		return dataWithoutLinks;
	}

	public Set<Long> getTotalIds() {
		return totalIds;
	}	
	
	public Set<Long> getqIdsNoLinks() {
		return qIdsNoLinks;
	}

	public HashMap<Long, Set<Long>> getLinks() {
		return links;
	}


	public Set<Long> getqIds() {
		return qIds;
	}

	public boolean isFirstDedup() {
		return firstDedup;
	}

	public Set<Long> getLinkedIds(Map<Long, Set<Long>> links, Set<Long> qIds) {

        Set<Long> linkedIds = new HashSet<>();
        Set<Set<Long>> sublinks = links.entrySet().stream().filter(entry -> {
            return qIds.contains(entry.getKey());
        }).map(entry -> {
            return entry.getValue();
        }).collect(Collectors.toSet());
        for (Set<Long> sublink : sublinks) {
            linkedIds.addAll(sublink);
        }
        linkedIds.removeAll(qIds);

        return linkedIds;
    }

    private HashMap<Long, String[]> getExtraData(HashMap<Long, String[]> dataWithLinks, Set<Long> linkedIds, RawFileService rawFileService) {
		HashMap<Long, String[]> extraData = new HashMap<>();
		for(Long id : linkedIds) {
			try {
				extraData.put(id, rawFileService.getObject(id));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
        return mergeMaps(dataWithLinks, extraData);
    }
    
    public HashMap<Long, String[]> mergeMaps(HashMap<Long, String[]> queryData, HashMap<Long, String[]> map2) {
        queryData.forEach(
                (key, value) -> map2.put(key, value)
        );
        return map2;
    }
    
    public void divideQueryData() {
    	// Check for links and remove qIds that have links
        dataWithLinks = new HashMap<>();
        totalIds = new HashSet<>();

        /* If there are links then we get all ids that are in the links HashMap (both on keys and the values).
         * Then we get all these data and put it onto the dataWithLinks hashMap.
         * Now we have two hashmaps 1) dataWithLinks, queryData = data without links.
         * After we deduplicate queryData, we will merge these two tables.
         */
        if (!firstDedup) {
            // Clear links and keep only qIds
            Set<Long> linkedIds = getLinkedIds(links, qIds); // Get extra Link Ids that are not in queryData
            dataWithLinks = (HashMap<Long, String[]>) links.keySet().stream()
                    .filter(dataWithoutLinks::containsKey)
                    .collect(Collectors.toMap(Function.identity(), dataWithoutLinks::get));
            dataWithLinks = getExtraData(dataWithLinks, linkedIds, rawFileService);
            dataWithoutLinks.keySet().removeAll(links.keySet());
            qIdsNoLinks = MapUtilities.deepCopySet(dataWithoutLinks.keySet());
            totalIds.addAll(linkedIds);  // Add links back
        }
        
    }
    
    public HashMap<Long, String[]> computeQueryData() {
  
    	dataWithoutLinks = qIds.stream().collect(Collectors.toMap(offset -> offset, offset -> {
            try {
                return rawFileService.getObject(offset);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return null;
        }, (left, right) -> right, HashMap::new));
        return dataWithoutLinks;
    }
    
   
	
}
