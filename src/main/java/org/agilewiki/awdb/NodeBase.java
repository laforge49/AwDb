package org.agilewiki.awdb;

import org.agilewiki.awdb.db.immutable.FactoryRegistry;

import java.util.List;
import java.util.NavigableMap;

/**
 * Base class for Node.
 */
public class NodeBase implements Node {
    private String nodeId;
    private long timestamp;
    private NodeData innerReference = null;
    private NodeData outerReference = null;

    public static AwDb getAwDb() {
        return AwDb.getAwDb();
    }

    public NodeBase(String nodeId, long timestamp) {
        if (nodeId != null) {
            initialize(nodeId, timestamp);
        }
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    protected void initialize(String nodeId, long timestamp) {
        this.nodeId = nodeId;
        this.timestamp = timestamp;
        innerReference = getAwDb().newNodeData(nodeId, timestamp);
        outerReference = innerReference;
    }

    public NodeData getNodeData() {
        if (getAwDb().isPrivileged())
            return innerReference;
        return outerReference;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean isLatestTime() {
        return timestamp == FactoryRegistry.MAX_TIMESTAMP;
    }

    @Override
    public void endTransaction() {
        outerReference = innerReference;
    }

    @Override
    public void reset() {
        innerReference = outerReference;
    }

    private void prep() {
        if (!isLatestTime()) {
            throw new IllegalStateException("No updates except with latest time");
        }
        if (innerReference == outerReference) {
            innerReference = new NodeData(innerReference);
            getAwDb().updated(this);
        }
    }

    @Override
    public void clearMap() {
        prep();
        innerReference.clearMap();
    }

    @Override
    public void set(String key, Object value) {
        prep();
        innerReference.set(key, value);
    }

    @Override
    public Object get(String key) {
        return getNodeData().get(key);
    }

    @Override
    public List getFlatList(String key) {
        return getNodeData().getFlatList(key);
    }

    @Override
    public NavigableMap<Comparable, List> getFlatMap() {
        return getNodeData().getFlatMap();
    }

    @Override
    public void createSecondaryId(String keyId, String valueId) {
        prep();
        innerReference.createSecondaryId(keyId, valueId);
    }

    @Override
    public void removeSecondaryId(String keyId, String valueId) {
        prep();
        innerReference.removeSecondaryId(keyId, valueId);
    }

    @Override
    public Iterable<String> nodeKeyIdIterable() {
        return getNodeData().nodeKeyIdIterable();
    }

    @Override
    public String getNodeValue(String keyId) {
        return getNodeData().getNodeValue(keyId);
    }

    @Override
    public boolean nodeHasKeyId(String keyId) {
        return getNodeData().nodeHasKeyId(keyId);
    }

    @Override
    public boolean nodeHasValueId(String keyId, String valueId) {
        return getNodeData().nodeHasValueId(keyId, valueId);
    }

    @Override
    public Iterable<String> nodeValueIdIterable(String keyId) {
        return getNodeData().nodeValueIdIterable(keyId);
    }

    @Override
    public void createLnk1(String labelId, String destinationNodeId) {
        prep();
        innerReference.createLnk1(labelId, destinationNodeId);
    }

    @Override
    public void removeLnk1(String labelId, String destinationNodeId) {
        prep();
        innerReference.removeLnk1(labelId, destinationNodeId);
    }

    @Override
    public Iterable<String> label1IdIterable() {
        return getNodeData().label1IdIterable();
    }

    @Override
    public boolean hasLabel1(String label1Id) {
        return getNodeData().hasLabel1(label1Id);
    }

    @Override
    public boolean hasDestination(String label1Id, String destinationId) {
        return getNodeData().hasDestination(label1Id, destinationId);
    }

    @Override
    public Iterable<String> destinationIdIterable(String label1Id) {
        return getNodeData().destinationIdIterable(label1Id);
    }
}
