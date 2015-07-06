package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.GenerativeNode;
import org.agilewiki.awdb.NodeBase;
import org.agilewiki.awdb.db.ids.NameId;
import org.agilewiki.awdb.db.immutable.collections.MapNode;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.Transaction;

public class JournalEntry_Node extends NodeBase implements Transaction, GenerativeNode {

    public JournalEntry_Node() {
        super(null, 0L);
    }

    public JournalEntry_Node(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public final void transform(Db db, MapNode tMapNode) {
        transformInit(db, tMapNode);
        process(db, tMapNode);
        getAwDb().addNode(this);
    }

    public void transformInit(Db db, MapNode tMapNode) {
        String nodeId = db.getJEName();
        String transactionName = tMapNode.get(NameId.TRANSACTION_NAME).toString();
        String nodeTypeId = NameId.generate(transactionName + ".node");
        String userId = (String) tMapNode.get(NameId.USER_KEY);
        createNode(nodeId, nodeTypeId, userId, Realm_NodeFactory.SYSTEM_REALM_ID);
    }

    public void process(Db db, MapNode tMapNode) {
        throw new UnsupportedOperationException();
    }
}
