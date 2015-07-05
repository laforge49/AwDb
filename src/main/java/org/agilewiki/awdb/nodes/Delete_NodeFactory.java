package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Delete_NodeFactory extends JournalEntry_NodeFactory {
    public final static String ID = "$ndelete.node";

    public static void create(AwDb awDb) {
        awDb.addTimelessNode(new Delete_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP));
    }

    public Delete_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node instantiateNode(String nodeId, long timestamp) {
        return new Delete_Node(nodeId, timestamp);
    }
}
