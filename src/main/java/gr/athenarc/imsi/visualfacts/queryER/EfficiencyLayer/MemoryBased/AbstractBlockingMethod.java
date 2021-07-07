package gr.athenarc.imsi.visualfacts.queryER.EfficiencyLayer.MemoryBased;

import java.util.List;
import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;


public abstract class AbstractBlockingMethod {
    
    private final String name;
    
    public AbstractBlockingMethod(String nm) {
        name = nm;
    }

    public String getName() {
        return name;
    }
    
    public abstract List<AbstractBlock> buildBlocks();
    public abstract void buildQueryBlocks();
    
}