package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.NodeBase;

public class Domain_Node extends NodeBase {

    public Domain_Node(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public String getRealmId() {
        return Realm_NodeFactory.USER_REALM_ID;
    }
}
