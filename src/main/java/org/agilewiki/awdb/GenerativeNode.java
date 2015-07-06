package org.agilewiki.awdb;

public interface GenerativeNode extends Node {
    void createNode(String nodeId, String nodeTypeId, String userId);
}
