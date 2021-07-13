package gr.athenarc.imsi.visualfacts.queryER;

import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.config.ERConfig;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.DecomposedBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.UnilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.Converter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class QueryTokenMap {
    private static final Logger LOG = LogManager.getLogger(QueryTokenMap.class);

    Map<String, Set<String>> map = new HashMap<>();

    Schema schema;

    public QueryTokenMap(Schema schema) {
        this.schema = schema;
    }

    public Map<String, Set<String>> joinTokenMap() {
		TokenMap tokenMap = new TokenMap(this.schema);
		Map<String, Set<String>> globalTokenMap = tokenMap.map;
		Predicate<Map.Entry<String, Set<String>>> predicate = entry -> map.keySet().contains(entry.getKey()); // note this is java.util.function.Predicate
		Map<String, Set<String>> extendedQueryMap = globalTokenMap.entrySet().stream().filter(predicate)
		        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		return extendedQueryMap;
    }
    
    public List<AbstractBlock> createExtendedBlockIndex(Map<String, Set<Long>> extendedTokenMap){
    	return parseIndex(extendedTokenMap);
    	
    }
    
    public static List<AbstractBlock> parseIndex(Map<String, Set<Long>> invertedIndex) {
		List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
		for (Entry<String, Set<Long>> term : invertedIndex.entrySet()) {
			if (1 < term.getValue().size()) {
				long[] idsArray = Converter.convertSetToArray(term.getValue());
				UnilateralBlock uBlock = new UnilateralBlock(idsArray);
				blocks.add(uBlock);
			}
		}
		invertedIndex.clear();
		return blocks;
	}
    
    public Set<Long> blocksToEntities(List<AbstractBlock> blocks){
		Set<Long> joinedEntityIds = new HashSet<>();
		for(AbstractBlock block : blocks) {
			UnilateralBlock uBlock = (UnilateralBlock) block;
			long[] entities = uBlock.getEntities();
			joinedEntityIds.addAll(Arrays.stream(entities).boxed().collect(Collectors.toSet()));
		}
		return joinedEntityIds;
	}
	

	public Set<Long> blocksToEntitiesD(List<AbstractBlock> blocks){
		Set<Long> joinedEntityIds = new HashSet<>();
		for(AbstractBlock block : blocks) {
			DecomposedBlock dBlock = (DecomposedBlock) block;

			long[] entities1 = dBlock.getEntities1();
			long[] entities2 = dBlock.getEntities2();
			joinedEntityIds.addAll(Arrays.stream(entities1).boxed().collect(Collectors.toSet()));
			joinedEntityIds.addAll(Arrays.stream(entities2).boxed().collect(Collectors.toSet()));

		}
		return joinedEntityIds;
	}
	
    public void processQueryObject(String[] object) {
        for (CategoricalColumn categoricalColumn : schema.getCategoricalColumns()) {
            String value = object[categoricalColumn.getIndex()];
            if (value == null)
                continue;
            String cleanValue = value.replaceAll("_", " ").trim().replaceAll("\\s*,\\s*$", "")
                    .toLowerCase();
            for (String token : cleanValue.split("[\\W_]")) {
                if (2 < token.trim().length()) {
                    if (ERConfig.getStopwords().contains(token.toLowerCase()))
                        continue;
                    Set<String> values = map.computeIfAbsent(token.trim(),
                            x -> new HashSet<>());

                    values.add(value);
                }
            }
        }
    }
}