package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.NodeBase;
import org.agilewiki.awdb.NodeFactory;

public abstract class Metadata_Node extends NodeBase implements NodeFactory {
    public Metadata_Node(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }
}
