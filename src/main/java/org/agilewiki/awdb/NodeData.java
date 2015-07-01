package org.agilewiki.awdb;

import org.agilewiki.awdb.db.ids.composites.Link1Id;
import org.agilewiki.awdb.db.ids.composites.SecondaryId;
import org.agilewiki.awdb.db.immutable.collections.VersionedMapNode;
import org.agilewiki.awdb.db.virtualcow.Db;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class NodeData {
    public final String nodeId;
    public final long timestamp;
    private final Db db;
    private ConcurrentSkipListMap<Comparable, List> atts;
    private ConcurrentSkipListMap<String, ConcurrentSkipListSet> keys;
    private ConcurrentSkipListMap<String, ConcurrentSkipListSet> lnk1s;

    public NodeData(Db db, String nodeId, long timestamp) {
        this.db = db;
        this.nodeId = nodeId;
        this.timestamp = timestamp;

        VersionedMapNode avmn = db.get(nodeId);
        if (avmn == null)
            atts = new ConcurrentSkipListMap<>();
        else
            atts = new ConcurrentSkipListMap<>(avmn.flatMap(timestamp));

        keys = new ConcurrentSkipListMap<>();
        for (String keyId : SecondaryId.typeIdIterable(db, nodeId)) {
            String secondaryInv = SecondaryId.secondaryInv(nodeId, keyId);
            for (String valueId : db.keysIterable(secondaryInv, timestamp)) {
                if (SecondaryId.hasSecondaryId(db, nodeId, keyId, valueId, timestamp)) {
                    ConcurrentSkipListSet values = keys.get(keyId);
                    if (values == null) {
                        values = new ConcurrentSkipListSet<>();
                        keys.put(keyId, values);
                    }
                    values.add(valueId);
                }
            }
        }

        lnk1s = new ConcurrentSkipListMap<>();
        for (String labelId : Link1Id.link1LabelIdIterable(db, nodeId)) {
            for (String destinationId : Link1Id.link1IdIterable(db, nodeId, labelId, timestamp)) {
                if (Link1Id.hasLink1(db, nodeId, labelId, destinationId, timestamp)) {
                    ConcurrentSkipListSet destinations = lnk1s.get(labelId);
                    if (destinations == null) {
                        destinations = new ConcurrentSkipListSet();
                        lnk1s.put(labelId, destinations);
                    }
                    destinations.add(destinationId);
                }
            }
        }
    }

    public NodeData(NodeData old) {
        db = old.db;
        nodeId = old.nodeId;
        this.timestamp = old.timestamp;

        atts = new ConcurrentSkipListMap<>();
        for (Comparable attId : old.atts.keySet()) {
            List l = old.atts.get(attId);
            atts.put(attId, new ArrayList<>(l));
        }

        keys = new ConcurrentSkipListMap<>();
        for (String keyId : old.keys.keySet()) {
            ConcurrentSkipListSet s = old.keys.get(keyId);
            keys.put(keyId, new ConcurrentSkipListSet<>(s));
        }

        lnk1s = new ConcurrentSkipListMap<>();
        for (String label1Id : old.lnk1s.keySet()) {
            ConcurrentSkipListSet s = old.lnk1s.get(label1Id);
            lnk1s.put(label1Id, new ConcurrentSkipListSet<>(s));
        }
    }

    public void clearMap() {
        db.clearMap(nodeId);
        atts.clear();
    }

    public void set(String key, Object value) {
        db.set(nodeId, key, value);
        List l = atts.get(key);
        if (l == null) {
            l = new ArrayList();
            atts.put(key, l);
        }
        l.add(value);
    }

    public Object get(String key) {
        List l = atts.get(key);
        if (l == null)
            return null;
        return l.get(0);
    }

    public List getFlatList(String key) {
        List l = atts.get(key);
        if (l == null)
            return new ArrayList();
        return new ArrayList(l);
    }

    public NavigableMap<Comparable, List> getFlatMap() {
        return new TreeMap<>(atts);
    }

    public void createSecondaryId(String keyId, String valueId) {
        SecondaryId.createSecondaryId(db, nodeId, SecondaryId.secondaryId(keyId, valueId));
        ConcurrentSkipListSet s = keys.get(keyId);
        if (s == null) {
            s = new ConcurrentSkipListSet();
            keys.put(keyId, s);
        }
        s.add(valueId);
    }

    public void removeSecondaryId(String keyId, String valueId) {
        SecondaryId.removeSecondaryId(db, nodeId, SecondaryId.secondaryId(keyId, valueId));
        ConcurrentSkipListSet s = keys.get(keyId);
        if (s == null)
            return;
        s.remove(valueId);
    }

    public Iterable<String> nodeKeyIdIterable() {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return keys.keySet().iterator();
            }
        };
    }

    public String getNodeValue(String keyId) {
        Iterator<String> keyValueIdIterator = keyValueIdIterator(keyId);
        if (!keyValueIdIterator.hasNext())
            return null;
        return keyValueIdIterator.next();
    }

    public boolean nodeHasKeyId(String keyId) {
        return keys.containsKey(keyId);
    }

    public boolean nodeHasValueId(String keyId, String valueId) {
        NavigableSet<String> s = keys.get(keyId);
        if (s == null)
            return false;
        return s.contains(valueId);
    }

    Iterator<String> keyValueIdIterator(String keyId) {
        ConcurrentSkipListSet s = keys.get(keyId);
        if (s == null)
            s = new ConcurrentSkipListSet();
        return s.iterator();
    }

    public Iterable<String> nodeValueIdIterable(String keyId) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return keyValueIdIterator(keyId);
            }
        };
    }

    public void createLnk1(String labelId, String destinationNodeId) {
        Link1Id.createLink1(db, nodeId, labelId, destinationNodeId);
    }

    public void removeLnk1(String labelId, String destinationNodeId) {
        Link1Id.removeLink1(db, nodeId, labelId, destinationNodeId);
    }

    public Iterable<String> label1IdIterable() {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return lnk1s.keySet().iterator();
            }
        };
    }

    public boolean hasLabel1(String label1Id) {
        return lnk1s.containsKey(label1Id);
    }

    public boolean hasDestination(String label1Id, String destinationId) {
        NavigableSet<String> s = lnk1s.get(label1Id);
        if (s == null)
            return false;
        return s.contains(destinationId);
    }

    Iterable<String> destinationIdIterable(String label1Id) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                ConcurrentSkipListSet s = lnk1s.get(label1Id);
                if (s == null)
                    s = new ConcurrentSkipListSet();
                return s.iterator();
            }
        };
    }
}
