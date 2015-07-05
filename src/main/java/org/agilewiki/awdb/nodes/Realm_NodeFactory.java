package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Realm_NodeFactory extends Node_Node {
    public final static String ID = "$nrealm.node";

    public static void create(AwDb awDb) {
        awDb.addTimelessNode(new Realm_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP));
    }

    public Realm_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node instantiateNode(String nodeId, long timestamp) {
        return new Realm_Node(nodeId, timestamp);
    }
}
