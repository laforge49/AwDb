package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.Delete;
import org.agilewiki.awdb.db.ids.NameId;
import org.agilewiki.awdb.db.immutable.collections.MapNode;
import org.agilewiki.awdb.db.virtualcow.Db;

/**
 * Delete a user.
 */
public class Delete_Node extends JournalEntry_Node {
    public final static String NAME = "delete";

    public Delete_Node() {
    }

    public Delete_Node(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public void process(Db db, MapNode mapNode) {
        String id = (String) mapNode.get(NameId.AN_ID);
        Delete.delete(id);
    }
}
