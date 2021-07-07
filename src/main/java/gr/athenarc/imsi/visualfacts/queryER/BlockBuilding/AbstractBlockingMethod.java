package gr.athenarc.imsi.visualfacts.queryER.BlockBuilding;

import gr.athenarc.imsi.visualfacts.queryER.DataStructures.AbstractBlock;
import java.util.List;



public abstract class AbstractBlockingMethod {

    private final String name;

    public AbstractBlockingMethod(String nm) {
        name = nm;
    }

    public String getName() {
        return name;
    }

    public abstract List<AbstractBlock> buildBlocks();
}
