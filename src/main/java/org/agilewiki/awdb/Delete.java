package org.agilewiki.awdb;

import org.agilewiki.awdb.nodes.Key_NodeFactory;

import java.util.ArrayDeque;

/**
 * Delete.
 */
public class Delete {
    public static void delete(String id) {
        AwDb awDb = AwDb.getAwDb();
        ArrayDeque<String> ids = new ArrayDeque<String>();
        deleter(awDb, ids, id);
        while (!ids.isEmpty()) {
            id = ids.removeLast();
            awDb.clearMap(id);
            for (String lnkTyp : awDb.originLabelIdIterable(id)) {
                for (String tId : awDb.destinationIdIterable(id, lnkTyp, awDb.getDbTimestamp())) {
                    awDb.removeLnk1(id, lnkTyp, tId);
                }
            }
            for (String lnkTyp : awDb.targetLabelInvIterable(id)) {
                for (String oId : awDb.originIdIterable(id, lnkTyp, awDb.getDbTimestamp())) {
                    awDb.removeLnk1(oId, lnkTyp, id);
                }
            }
            for (String keyId : awDb.nodeKeyIdIterable(id)) {
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
