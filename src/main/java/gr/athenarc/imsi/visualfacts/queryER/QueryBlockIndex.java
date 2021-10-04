package gr.athenarc.imsi.visualfacts.queryER;

import gr.athenarc.imsi.visualfacts.Point;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.query.QueryResults;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.DecomposedBlock;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.UnilateralBlock;
import gr.athenarc.imsi.visualfacts.queryER.Utilities.Converter;
import gr.athenarc.imsi.visualfacts.util.RawFileService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class QueryBlockIndex extends BlockIndex {
    private static final Logger LOG = LogManager.getLogger(QueryBlockIndex.class);

    RawFileService rawFileService;

    Schema schema;

    public QueryBlockIndex(Schema schema, RawFileService rawFileService) {
        super(schema);
        this.rawFileService = rawFileService;
    }

    public static Set<Long> blocksToEntities(List<AbstractBlock> blocks) {
        Set<Long> joinedEntityIds = new HashSet<>();
        for (AbstractBlock block : blocks) {
            UnilateralBlock uBlock = (UnilateralBlock) block;
            long[] entities = uBlock.getEntities();
            joinedEntityIds.addAll(Arrays.stream(entities).boxed().collect(Collectors.toSet()));
        }
        return joinedEntityIds;
    }

    public static List<AbstractBlock> parseIndex(Map<String, Set<Point>> invertedIndex) {
        List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
        for (Entry<String, Set<Point>> term : invertedIndex.entrySet()) {
            if (1 < term.getValue().size()) {
                long[] idsArray = Converter.convertSetToArray(term.getValue().stream().map(Point::getFileOffset).collect(Collectors.toSet()));
                UnilateralBlock uBlock = new UnilateralBlock(idsArray);
                blocks.add(uBlock);
            }
        }
        invertedIndex.clear();
        return blocks;
    }

    public static Set<Long> blocksToEntitiesD(List<AbstractBlock> blocks) {
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

    public void processQueryResults(HashMap<Long, String[]> dataWithoutLinks, Map<String, Set<Point>> invertedIndex) throws IOException {
        Set<String> queryTokens = new HashSet<>();
        for (String[] row : dataWithoutLinks.values()) {
            queryTokens.addAll(parseRowTokens(row));
        }
        this.invertedIndex = invertedIndex.entrySet().stream().filter(e -> queryTokens.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}