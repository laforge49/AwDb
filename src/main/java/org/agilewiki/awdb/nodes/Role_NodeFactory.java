package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Role_NodeFactory extends Node_Node {
    public final static String ID = "$nrole.node";

    public static void create(AwDb awDb) throws Exception {
        Role_NodeFactory role_nodeFactory = new Role_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP);
        awDb.addTimelessNode(role_nodeFactory);
    }

    public Role_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node instantiateNode(String nodeId, long timestamp) {
        return new Role_Node(nodeId, timestamp);
    }
}
