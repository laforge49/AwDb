package org.agilewiki.awdb;

import org.agilewiki.awdb.db.ids.NameId;
import org.agilewiki.awdb.db.immutable.collections.MapNode;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.nodes.JournalEntry_Node;

public class AwDbTran extends JournalEntry_Node {
    public final static String NAME = "awdbTran";

    public AwDbTran() {
    }

    public AwDbTran(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }
    @Override
    public void process(Db db, MapNode mapNode) {
        db.set(NameId.generate("x"), "y", 3);
    }
}
