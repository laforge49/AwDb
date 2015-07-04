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
        User_NodeFactory.create(awDb);
    }

    public static void define(String nodeId, String nodeType, String superType, String... attributes) {
        AwDb awDb = getAwDb();
        if (nodeType != null) {
            awDb.createSecondaryId(nodeId, Key_NodeFactory.NODETYPE_ID, nodeType);
        }
        if (superType != null) {
            awDb.createSecondaryId(nodeId, Key_NodeFactory.SUPERTYPE_ID, superType);
        }
        for (String attributeName : attributes) {
            Attribute_NodeFactory.define(attributeName, nodeId);
        }
    }

    public Node_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node createNode(String nodeId, long timestamp) {
        throw new UnsupportedOperationException(nodeId);
    }
}
