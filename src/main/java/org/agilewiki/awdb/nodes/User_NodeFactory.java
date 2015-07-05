package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class User_NodeFactory extends Node_Node {
    public final static String ID = "$nuser.node";
    public final static String SYSTEM_USER_ID = "$nsystem.user";

    public static void create(AwDb awDb) {
        User_NodeFactory user_nodeFactory = new User_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP);
        awDb.addTimelessNode(user_nodeFactory);
        awDb.addTimelessNode(user_nodeFactory.instantiateNode(SYSTEM_USER_ID, FactoryRegistry.MAX_TIMESTAMP));
    }

    public User_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node instantiateNode(String nodeId, long timestamp) {
        return new User_Node(nodeId, timestamp);
    }
}
