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
        System.out.println("Ribbit!");
    }
}
