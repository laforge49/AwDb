package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Group_NodeFactory extends Node_Node {
    public final static String ID = "$ngroup.node";
    public final static String USERS_GROUP_ID = "$nusers.group";
    public final static String ADMINS_GROUP_ID = "$nadmins.group";

    public static void create(AwDb awDb) {
        Group_NodeFactory group_nodeFactory = new Group_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP);
        awDb.addTimelessNode(group_nodeFactory);
        awDb.addTimelessNode(group_nodeFactory.instantiateNode(USERS_GROUP_ID, FactoryRegistry.MAX_TIMESTAMP));
        awDb.addTimelessNode(group_nodeFactory.instantiateNode(ADMINS_GROUP_ID, FactoryRegistry.MAX_TIMESTAMP));
    }

    public Group_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node instantiateNode(String nodeId, long timestamp) {
        return new Group_Node(nodeId, timestamp);
    }
}
