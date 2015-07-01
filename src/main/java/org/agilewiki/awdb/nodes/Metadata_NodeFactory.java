package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Metadata_NodeFactory extends Node_Node {
    public final static String ID = "$nmetadata.node";

    public static void create(AwDb awDb)
            throws Exception {
        awDb.addTimelessNode(new Metadata_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP));
        Node_NodeFactory.create(awDb);
        Lnk1_NodeFactory.create(awDb);
        Key_NodeFactory.create(awDb);
    }

    public Metadata_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node createNode(String nodeId, long timestamp) {
        throw new UnsupportedOperationException();
    }
}
