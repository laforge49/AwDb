package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.ids.NameId;
import org.agilewiki.awdb.db.immutable.collections.MapNode;

public class BigMapTran implements Transaction {
    /**
     * Transforms a map list.
     *
     * @param db        The database to be updated.
     * @param tMapNode  The durable content of the transaction.
     */
    @Override
    public void transform(Db db, MapNode tMapNode) {
        int k = (Integer) tMapNode.getList("k").get(0);
        int I = (Integer) tMapNode.getList("I").get(0);
        for (int i = 0; i < I; i++) {
            db.set(NameId.generate("" + (k * 10000000 + i)), "", 3);
        }
    }
}
