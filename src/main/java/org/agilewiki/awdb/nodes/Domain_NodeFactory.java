package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Domain_NodeFactory extends Node_Node {
    public final static String ID = "$ndomain.node";

    public static void create(AwDb awDb) {
        Domain_NodeFactory domain_nodeFactory = new Domain_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP);
        awDb.addTimelessNode(domain_nodeFactory);
    }

    public Domain_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node instantiateNode(String nodeId, long timestamp) {
        return new Domain_Node(nodeId, timestamp);
    }
}
