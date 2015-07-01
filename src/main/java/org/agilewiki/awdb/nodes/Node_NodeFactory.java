package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Node_NodeFactory extends Metadata_NodeFactory {
    public final static String ID = "$nnode.node";

    public static void create(AwDb awDb)
            throws Exception {
        awDb.addTimelessNode(new Node_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP));
        JournalEntry_NodeFactory.create(awDb);
        Attribute_NodeFactory.create(awDb);
    }

    public Node_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node createNode(String nodeId, long timestamp) {
        throw new UnsupportedOperationException(nodeId);
    }
}
