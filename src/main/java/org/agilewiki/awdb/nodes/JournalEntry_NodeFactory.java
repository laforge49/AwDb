package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class JournalEntry_NodeFactory extends Node_Node {
    public final static String ID = "$njournalEntry.node";

    public static void create(AwDb awDb) {
        awDb.addTimelessNode(new JournalEntry_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP));
    }

    public JournalEntry_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node createNode(String nodeId, long timestamp) {
        throw new UnsupportedOperationException();
    }
}
