package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class InitializeDatabase_NodeFactory extends JournalEntry_NodeFactory {
    public final static String ID = "$ninitializeDatabase.node";

    public static void create(AwDb awDb) {
        awDb.addTimelessNode(new InitializeDatabase_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP));
        awDb.registerTransaction(InitializeDatabase_Node.NAME, InitializeDatabase_Node.class);
    }

    public InitializeDatabase_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node createNode(String nodeId, long timestamp) {
        return new InitializeDatabase_Node(nodeId, timestamp);
    }
}
