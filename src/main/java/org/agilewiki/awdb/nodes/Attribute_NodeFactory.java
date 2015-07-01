package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Attribute_NodeFactory extends Node_Node {
    public final static String ID = "$nattribute.node";

    public static void create(AwDb awDb) {
        awDb.addTimelessNode(new Attribute_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP));
    }

    public static void define(String attributeNameId, String nodeId) {
        AwDb awDb = getAwDb();
        String attributeId = AwDb.randomId.generate();
        awDb.set(attributeId, "$nsubject", attributeNameId + " - " + nodeId);
        awDb.createSecondaryId(attributeId, Key_NodeFactory.NODETYPE_ID, ID);
        awDb.createSecondaryId(attributeId, Key_NodeFactory.ATTRIBUTENAME_ID, attributeNameId);
        awDb.createLnk1(attributeId, Lnk1_NodeFactory.ATTRIBUTEOF_ID, nodeId);
    }

    public Attribute_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node createNode(String nodeId, long timestamp) {
        return new Attribute_Node(nodeId, timestamp);
    }
}
