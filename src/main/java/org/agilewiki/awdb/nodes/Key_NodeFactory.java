package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Key_NodeFactory extends Metadata_NodeFactory {
    public final static String ID = "$nkey.node";

    final public static String NODETYPE_ID = "$nnodeType";
    final public static String NODETYPE_KEY_ID = "$nnodeType.key";
    final public static String SUPERTYPE_ID = "$nsuperType";
    final public static String SUPERTYPE_KEY_ID = "$nsuperType.key";
    final public static String INVDEPENDENCY_ID = "$ninvDependency";
    final public static String INVDEPENDENCY_KEY_ID = "$ninvDependency.key";
    final public static String ATTRIBUTENAME_ID = "$nattributeName";
    final public static String ATTRIBUTENAME_KEY_ID = "$nattributeName.key";

    public static void create(AwDb awDb) {
        awDb.addTimelessNode(new Key_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP));
    }

    public Key_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node createNode(String nodeId, long timestamp) {
        return new Key_Node(nodeId, timestamp);
    }
}
