package gr.athenarc.imsi.visualfacts.queryER;

import gr.athenarc.imsi.visualfacts.CategoricalColumn;
import gr.athenarc.imsi.visualfacts.Point;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.config.ERConfig;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.DecomposedBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.UnilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.Converter;
import gr.athenarc.imsi.visualfacts.util.RawFileService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class QueryTokenMap {
    private static final Logger LOG = LogManager.getLogger(QueryTokenMap.class);

    Map<Integer, Map<String, Set<String>>> map = new HashMap<>();
    RawFileService rawFileService;

    Schema schema;

    public QueryTokenMap(Schema schema, RawFileService rawFileService) {
        this.schema = schema;
        this.rawFileService = rawFileService;
        for (CategoricalColumn categoricalColumn : schema.getCategoricalColumns()) {
            map.put(categoricalColumn.getIndex(), new HashMap<>());
        }
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

    public List<AbstractBlock> createExtendedBlockIndex(Map<String, Set<Long>> extendedTokenMap) {
        return parseIndex(extendedTokenMap);

    }

    public void processQueryResults(QueryResults queryResults, TokenMap globalTokenMap) {
        Set<String> tokens = queryResults.getPoints().stream().map(this::processQueryObject).flatMap(Set::stream).collect(Collectors.toSet());

        for (CategoricalColumn categoricalColumn : schema.getCategoricalColumns()) {
            map.put(categoricalColumn.getIndex(), globalTokenMap.map.get(categoricalColumn.getIndex()).entrySet().stream().filter(e -> tokens.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
    }


    private Set<String> processQueryObject(Point point) {
        String[] object = rawFileService.getObject(point.getFileOffset());
        Set<String> tokens = new HashSet<>();
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
                    tokens.add(value);
                }
            }
        }
        return tokens;
    }

    public Set<Long> blocksToEntities(List<AbstractBlock> blocks) {
        Set<Long> joinedEntityIds = new HashSet<>();
        for (AbstractBlock block : blocks) {
            UnilateralBlock uBlock = (UnilateralBlock) block;
            long[] entities = uBlock.getEntities();
            joinedEntityIds.addAll(Arrays.stream(entities).boxed().collect(Collectors.toSet()));
        }
        return joinedEntityIds;
    }


    public Set<Long> blocksToEntitiesD(List<AbstractBlock> blocks) {
        Set<Long> joinedEntityIds = new HashSet<>();
        for (AbstractBlock block : blocks) {
            DecomposedBlock dBlock = (DecomposedBlock) block;

            long[] entities1 = dBlock.getEntities1();
            long[] entities2 = dBlock.getEntities2();
            joinedEntityIds.addAll(Arrays.stream(entities1).boxed().collect(Collectors.toSet()));
            joinedEntityIds.addAll(Arrays.stream(entities2).boxed().collect(Collectors.toSet()));

        }
        return joinedEntityIds;
    }
}