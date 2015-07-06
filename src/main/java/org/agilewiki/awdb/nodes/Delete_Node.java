package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.db.ids.NameId;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.collections.MapNode;
import org.agilewiki.awdb.db.virtualcow.Db;

import java.util.ArrayDeque;

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
        delete(id);
    }

    /**
     * Delete.
     */
    public static void delete(String id) {
        AwDb awDb = AwDb.getAwDb();
        ArrayDeque<String> ids = new ArrayDeque<String>();
        deleter(awDb, ids, id);
        while (!ids.isEmpty()) {
            id = ids.removeLast();
            awDb.clearMap(id);
            for (String lnkTyp : awDb.originLabelIdIterable(id, FactoryRegistry.MAX_TIMESTAMP)) {
                for (String tId : awDb.destinationIdIterable(id, lnkTyp, awDb.getDbTimestamp())) {
                    awDb.removeLnk1(id, lnkTyp, tId);
                }
            }
            for (String lnkTyp : awDb.targetLabelInvIterable(id)) {
                for (String oId : awDb.originIdIterable(id, lnkTyp, awDb.getDbTimestamp())) {
                    awDb.removeLnk1(oId, lnkTyp, id);
                }
            }
            for (String keyId : awDb.nodeKeyIdIterable(id, FactoryRegistry.MAX_TIMESTAMP)) {
                for (String valueId : awDb.nodeValueIdIterable(id, keyId, awDb.getDbTimestamp())) {
                    awDb.removeSecondaryId(id, keyId, valueId);
                }
            }
        }
    }

    private static void deleter(AwDb awDb, ArrayDeque<String> ids, String id) {
        ids.addLast(id);
        for (String lnkTyp : awDb.targetLabelInvIterable(id)) {
            for (String oId : awDb.originIdIterable(id, lnkTyp, awDb.getDbTimestamp())) {
                if (awDb.nodeHasValueId(lnkTyp + ".lnk1",
                        Key_NodeFactory.INVDEPENDENCY_ID,
                        lnkTyp,
                        awDb.getDbTimestamp())) {
                    deleter(awDb, ids, oId);
                }
            }
        }
    }
}
