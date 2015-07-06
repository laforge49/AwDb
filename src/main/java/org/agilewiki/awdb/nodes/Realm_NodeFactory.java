package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Realm_NodeFactory extends Node_Node {
    public final static String ID = "$nrealm.node";
    public final static String SYSTEM_REALM_ID = "$nsystem.realm";
    public final static String USER_REALM_ID = "$nuser.realm";

    public static void create(AwDb awDb) {
        Realm_NodeFactory realm_nodeFactory = new Realm_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP);
        awDb.addTimelessNode(realm_nodeFactory);
        awDb.addTimelessNode(realm_nodeFactory.instantiateNode(SYSTEM_REALM_ID, FactoryRegistry.MAX_TIMESTAMP));
        awDb.addTimelessNode(realm_nodeFactory.instantiateNode(USER_REALM_ID, FactoryRegistry.MAX_TIMESTAMP));
    }

    public Realm_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node instantiateNode(String nodeId, long timestamp) {
        return new Realm_Node(nodeId, timestamp);
    }
}
