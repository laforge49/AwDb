package org.agilewiki.awdb;

import java.util.List;
import java.util.NavigableMap;

public class NullNode implements Node {
    public final static NullNode singleton = new NullNode();

    private NullNode() {
    }

    @Override
    public String getNodeId() {
        return null;
    }

    @Override
    public NodeData getNodeData() {
        return null;
    }

    @Override
    public long getTimestamp() {
        return 0;
    }

    @Override
    public boolean isLatestTime() {
        return false;
    }

    @Override
    public void endTransaction() {

    }

    @Override
    public void reset() {

    }

    @Override
    public void clearMap() {

    }

    @Override
    public void set(String key, Object value) {

    }

    @Override
    public Object get(String key) {
        return null;
    }

    @Override
    public List getFlatList(String key) {
        return null;
    }

    @Override
    public NavigableMap<Comparable, List> getFlatMap() {
        return null;
    }

    @Override
    public void createSecondaryId(String keyId, String valueId) {

    }

    @Override
    public void removeSecondaryId(String keyId, String valueId) {

    }

    @Override
    public Iterable<String> nodeKeyIdIterable() {
        return null;
    }

    @Override
    public String getNodeValue(String keyId) {
        return null;
    }

    @Override
    public boolean nodeHasKeyId(String keyId) {
        return false;
    }

    @Override
    public boolean nodeHasValueId(String keyId, String valueId) {
        return false;
    }

    @Override
    public Iterable<String> nodeValueIdIterable(String keyId) {
        return null;
    }

    @Override
    public void createLnk1(String labelId, String destinationNodeId) {

    }

    @Override
    public void removeLnk1(String labelId, String destinationNodeId) {

    }

    @Override
    public Iterable<String> label1IdIterable() {
        return null;
    }

    @Override
    public boolean hasLabel1(String label1Id) {
        return false;
    }

    @Override
    public boolean hasDestination(String label1Id, String destinationId) {
        return false;
    }

    @Override
    public Iterable<String> destinationIdIterable(String label1Id) {
        return null;
    }
}
