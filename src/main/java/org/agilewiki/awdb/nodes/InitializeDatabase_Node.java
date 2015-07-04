package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.db.immutable.collections.MapNode;
import org.agilewiki.awdb.db.virtualcow.Db;

public class InitializeDatabase_Node extends JournalEntry_Node {
    public final static String NAME = "initializeDatabase";

    public InitializeDatabase_Node() {
    }

    public InitializeDatabase_Node(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    public static String update(AwDb awDb)
            throws Exception {
        MapNode mn = awDb.nilMap;
        return awDb.update(NAME, mn).call();
    }

    @Override
    public void process(Db db, MapNode tMapNode) {
        Key_NodeFactory.define(Key_NodeFactory.NODETYPE_KEY_ID, Node_NodeFactory.ID);
        Key_NodeFactory.define(Key_NodeFactory.SUPERTYPE_KEY_ID, Node_NodeFactory.ID);
        Key_NodeFactory.define(Key_NodeFactory.ATTRIBUTENAME_KEY_ID, Attribute_NodeFactory.ID);
        Key_NodeFactory.define(Key_NodeFactory.INVDEPENDENCY_KEY_ID, Lnk1_NodeFactory.ID);
        Key_NodeFactory.define(Key_NodeFactory.SUBJECT_KEY_ID, Node_NodeFactory.ID);

        Lnk1_NodeFactory.define(Lnk1_NodeFactory.TARGET_LNK1_ID, null, Node_NodeFactory.ID, Node_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.ATTRIBUTEOF_LNK1_ID, Lnk1_NodeFactory.ATTRIBUTEOF_ID, Attribute_NodeFactory.ID, Metadata_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.ORIGIN_LNK1_ID, null, Lnk1_NodeFactory.ID, Node_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.DESTINATION_LNK1_ID, null, Lnk1_NodeFactory.ID, Node_NodeFactory.ID);
    }
}
