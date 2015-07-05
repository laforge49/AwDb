package org.agilewiki.awdb;

/**
 * A factory to create a node.
 */
public interface NodeFactory extends Node {
    Node instantiateNode(String nodeId, long timestamp);
}
