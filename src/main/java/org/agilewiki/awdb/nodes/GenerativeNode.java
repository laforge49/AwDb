package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.Node;

public interface GenerativeNode extends Node {
    void createNode(String nodeId, String nodeTypeId, String userId, String RealmId);
}
