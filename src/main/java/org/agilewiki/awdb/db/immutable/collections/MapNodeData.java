package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import static java.lang.Math.min;

/**
 * The durable data elements of a map node.
 */
public class MapNodeData implements Releasable {

    /**
     * The node which holds this data.
     */
    public final MapNode thisNode;

    /**
     * Composite node depth--see AA Tree algorithm.
     */
    public final int level;

    /**
     * Left subtree node.
     */
    public final MapNode leftNode;

    /**
     * The list of the node.
     */
    public final ListNode listNode;

    /**
     * Right subtree node.
     */
    public final MapNode rightNode;

    /**
     * The list of the node.
     */
    public final Comparable key;

    /**
     * The factory for the key.
     */
    public final ImmutableFactory keyFactory;

    /**
     * Create the nil node data.
     *
     * @param thisNode The node which holds this data.
     */
    public MapNodeData(MapNode thisNode) {
        this.thisNode = thisNode;
        this.level = 0;
        this.leftNode = thisNode;
        this.listNode = thisNode.getRegistry().nilList;
        this.rightNode = thisNode;
        key = null;

        keyFactory = null;
    }

    /**
     * Create non-nill node data.
     *
     * @param thisNode  The node which holds this data.
     * @param level     Composite node depth--see AA Tree algorithm.
     * @param leftNode  Left subtree node.
     * @param listNode  The list of the node.
     * @param rightNode Right subtree node.
     * @param key       The key of node.
     */
    public MapNodeData(MapNode thisNode,
                       int level,
                       MapNode leftNode,
                       ListNode listNode,
                       MapNode rightNode,
                       Comparable key) {
        this.thisNode = thisNode;
        this.level = level;
        this.leftNode = leftNode;
        this.listNode = listNode;
        this.rightNode = rightNode;
        this.key = key;
        keyFactory = thisNode.getRegistry().getImmutableFactory(key);
    }

    /**
     * Create non-nill node data.
     *
     * @param thisNode   The node which holds this data.
     * @param byteBuffer Holds the serialized data.
     */
    public MapNodeData(MapNode thisNode, ByteBuffer byteBuffer) {
        this.thisNode = thisNode;
        level = byteBuffer.getInt();
        FactoryRegistry factoryRegistry = thisNode.getRegistry();
        ImmutableFactory f = factoryRegistry.readId(byteBuffer);
        leftNode = (MapNode) f.deserialize(byteBuffer);
        f = factoryRegistry.readId(byteBuffer);
        listNode = (ListNode) f.deserialize(byteBuffer);
        f = factoryRegistry.readId(byteBuffer);
        rightNode = (MapNode) f.deserialize(byteBuffer);
        keyFactory = factoryRegistry.readId(byteBuffer);
        key = (Comparable) keyFactory.deserialize(byteBuffer);
    }

    /**
     * Returns true if this is the data for the nil node.
     *
     * @return True if nil node.
     */
    public boolean isNil() {
        return level == 0;
    }

    /**
     * Returns the list for the node.
     *
     * @param key The key for the node.
     * @return The list, or null.
     */
    public ListNode getList(Comparable key) {
        if (isNil())
            return null;
        int c = key.compareTo(this.key);
        if (c < 0)
            return leftNode.getList(key);
        if (c == 0)
            return listNode;
        return rightNode.getList(key);
    }

    /**
     * AA Tree skew operation.
     *
     * @return Revised root node.
     */
    public MapNode skew() {
        if (isNil() || leftNode.isNil())
            return thisNode;
        MapNodeData leftData = leftNode.getData();
        if (leftData.level == level) {
            MapNode t = replaceLeft(leftData.rightNode);
            return leftData.replaceRight(t);
        } else
            return thisNode;
    }

    /**
     * AA Tree split
     *
     * @return The revised root node.
     */
    public MapNode split() {
        if (isNil() || rightNode.isNil())
            return thisNode;
        MapNodeData rightData = rightNode.getData();
        if (rightData.rightNode.isNil())
            return thisNode;
        if (level == rightData.rightNode.getData().level) {
            MapNode t = replaceRight(rightData.leftNode);
            return rightData.replaceLeft(rightData.level + 1, t);
        }
        return thisNode;
    }

    /**
     * Add a non-null value to the list.
     *
     * @param key   The key of the list.
     * @param ndx   Where to add the value.
     * @param value The value to be added.
     * @return The revised root node.
     */
    public MapNode add(Comparable key, int ndx, Object value) {
        MapNode t;
        int c = key.compareTo(this.key);
        if (c < 0) {
            t = replaceLeft(leftNode.add(key, ndx, value));
        } else if (c == 0) {
            return replace(listNode.add(ndx, value));
        } else {
            t = replaceRight(rightNode.add(key, ndx, value));
        }
        return t.getData().skew().getData().split();
    }

    private MapNode successor() {
        return rightNode.getData().leftMost();
    }

    private MapNode leftMost() {
        if (!leftNode.isNil())
            return leftNode.getData().leftMost();
        return thisNode;
    }

    private MapNode predecessor() {
        return leftNode.getData().rightMost();
    }

    private MapNode rightMost() {
        if (!rightNode.isNil())
            return rightNode.getData().rightMost();
        return thisNode;
    }

    private MapNode decreaseLevel() {
        MapNodeData rd = rightNode.getData();
        int shouldBe = min(leftNode.getData().level, rd.level) + 1;
        if (shouldBe < level) {
            MapNode r;
            if (shouldBe < rd.level)
                r = rd.replace(shouldBe);
            else
                r = rightNode;
            return replaceRight(shouldBe, r);
        }
        return thisNode;
    }

    public MapNode remove(Comparable key) {
        if (isNil())
            return thisNode;
        MapNodeFactory factory = thisNode.getRegistry().mapNodeFactory;
        int c = key.compareTo(this.key);
        MapNode t = thisNode;
        if (c > 0) {
            MapNode r = rightNode.remove(key);
            if (r != rightNode)
                t = replaceRight(r);
        } else if (c < 0) {
            MapNode l = leftNode.remove(key);
            if (l != leftNode)
                t = replaceLeft(l);
        } else {
            if (listNode instanceof Releasable)
                ((Releasable) listNode).releaseAll();
            MapNode nil = factory.nilMap;
            if (leftNode.isNil() && rightNode.isNil()) {
                return nil;
            }
            if (leftNode.isNil()) {
                MapNode l = successor();
                MapNodeData ld = l.getData();
                t = ld.replace(level, nil, rightNode.remove(ld.key));
            } else {
                MapNode l = predecessor();
                MapNodeData ld = l.getData();
                t = ld.replaceLeft(level, leftNode.remove(ld.key));
            }
        }
        t = t.getData().decreaseLevel().getData().skew();
        MapNodeData td = t.getData();
        MapNode r = td.rightNode.getData().skew();
        if (!r.isNil()) {
            MapNodeData rd = r.getData();
            MapNode rr = rd.rightNode.getData().skew();
            if (rd.rightNode != rr) {
                r = rd.replaceRight(rr);
            }
        }
        if (r != td.rightNode) {
            t = td.replaceRight(r);
        }
        t = t.getData().split();
        r = t.getData().rightNode.getData().split();
        td = t.getData();
        if (r != td.rightNode) {
            t = td.replaceRight(r);
        }
        return t;
    }

    /**
     * Delete a value from the list.
     *
     * @param key The key of the list.
     * @param ndx The index of the value.
     * @return The revised node.
     */
    public MapNode remove(Comparable key, int ndx) {
        if (key == null)
            throw new IllegalArgumentException("key may not be null");
        if (isNil())
            return thisNode;
        int c = key.compareTo(this.key);
        if (c < 0) {
            MapNode n = leftNode.remove(key, ndx);
            if (n == leftNode)
                return thisNode;
            return replaceLeft(n);
        }
        if (c > 0) {
            MapNode n = rightNode.remove(key, ndx);
            if (n == rightNode)
                return thisNode;
            return replaceRight(n);
        }
        ListNode n = listNode.remove(ndx);
        if (n == listNode)
            return thisNode;
        if (n.isNil())
            return remove(key);
        return replace(n);
    }

    /**
     * Delete the first occurance of a value from the list.
     *
     * @param key The key of the list.
     * @param x The value to remove.
     * @return The revised node.
     */
    public MapNode remove(Comparable key, Object x) {
        if (key == null)
            throw new IllegalArgumentException("key may not be null");
        if (isNil())
            return thisNode;
        int c = key.compareTo(this.key);
        if (c < 0) {
            MapNode n = leftNode.remove(key, x);
            if (n == leftNode)
                return thisNode;
            return replaceLeft(n);
        }
        if (c > 0) {
            MapNode n = rightNode.remove(key, x);
            if (n == rightNode)
                return thisNode;
            return replaceRight(n);
        }
        ListNode n = listNode.remove(x);
        if (n == listNode)
            return thisNode;
        if (n.isNil())
            return remove(key);
        return replace(n);
    }

    /**
     * Replace the list entries with a single value.
     *
     * @param key   The key of the list.
     * @param value The new value.
     * @return The revised node.
     */
    public MapNode set(Comparable key, Object value) {
        int c = key.compareTo(this.key);
        if (c < 0) {
            MapNode n = leftNode.set(key, value);
            return replaceLeft(n);
        } else if (c == 0) {
            listNode.releaseAll();
            ListNode n = thisNode.getRegistry().nilList.add(value);
            return replace(n);
        } else {
            MapNode n = rightNode.set(key, value);
            return replaceRight(n);
        }
    }

    /**
     * Builds a set of all keys with non-empty lists.
     *
     * @param keys The set being built.
     */
    public void flatKeys(NavigableSet<Comparable> keys) {
        if (isNil())
            return;
        leftNode.getData().flatKeys(keys);
        if (!listNode.isEmpty())
            keys.add(key);
        rightNode.getData().flatKeys(keys);
    }

    /**
     * Builds a map of all the keys and values.
     *
     * @param map The map being built.
     */
    public void flatMap(NavigableMap<Comparable, List> map) {
        if (isNil())
            return;
        leftNode.getData().flatMap(map);
        if (!listNode.isEmpty())
            map.put(key, listNode.flatList());
        rightNode.getData().flatMap(map);
    }

    protected MapNode addList(Comparable key, ListNode listNode) {
        if (listNode.isNil())
            return thisNode;
        if (isNil()) {
            return replace(1, listNode, key);
        }
        MapNode t;
        int c = key.compareTo(this.key);
        if (c < 0) {
            t = replaceLeft(leftNode.getData().addList(key, listNode));
        } else if (c == 0) {
            throw new IllegalArgumentException("duplicate key not supported");
        } else {
            t = replaceRight(rightNode.getData().addList(key, listNode));
        }
        return t.getData().skew().getData().split();
    }

    /**
     * Returns the count of all the keys in the map.
     *
     * @return The count of all the keys in the map.
     */
    public int totalSize() {
        if (isNil())
            return 0;
        return leftNode.totalSize() + 1 + rightNode.totalSize();
    }

    /**
     * Returns the count of all the keys.
     *
     * @return The size of the map.
     */
    public int size() {
        return totalSize();
    }

    /**
     * Returns the smallest key.
     *
     * @return The smallest key, or null.
     */
    public Comparable firstKey() {
        if (isNil())
            return null;
        Comparable k = leftNode.firstKey();
        if (k != null)
            return k;
        return key;
    }

    /**
     * Returns the largest key.
     *
     * @return The largest key, or null.
     */
    public Comparable lastKey() {
        if (isNil())
            return null;
        Comparable k = rightNode.lastKey();
        if (k != null)
            return k;
        return key;
    }

    /**
     * Returns the next greater key.
     *
     * @param key The given key.
     * @return The next greater key, or null.
     */
    public Comparable higherKey(Comparable key) {
        if (isNil())
            return null;
        int c = key.compareTo(this.key);
        if (c <= 0) {
            Comparable k = leftNode.higherKey(key);
            if (k != null)
                return k;
        }
        if (c < 0)
            return this.key;
        return rightNode.higherKey(key);
    }

    /**
     * Returns the key that is greater than or equal to the given key.
     *
     * @param key The given key.
     * @return The key greater than or equal to the given key, or null.
     */
    public Comparable ceilingKey(Comparable key) {
        if (isNil())
            return null;
        int c = key.compareTo(this.key);
        if (c < 0) {
            Comparable k = leftNode.ceilingKey(key);
            if (k != null)
                return k;
        }
        if (c <= 0)
            return this.key;
        return rightNode.ceilingKey(key);
    }

    /**
     * Returns the next smaller key.
     *
     * @param key The given key.
     * @return The next smaller key, or null.
     */
    public Comparable lowerKey(Comparable key) {
        if (isNil())
            return null;
        int c = key.compareTo(this.key);
        if (c >= 0) {
            Comparable k = rightNode.lowerKey(key);
            if (k != null)
                return k;
        }
        if (c > 0)
            return this.key;
        return leftNode.lowerKey(key);
    }

    /**
     * Returns the key that is smaller than or equal to the given key.
     *
     * @param key The given key.
     * @return The key smaller than or equal to the given key, or null.
     */
    public Comparable floorKey(Comparable key) {
        if (isNil())
            return null;
        int c = key.compareTo(this.key);
        if (c > 0) {
            Comparable k = rightNode.floorKey(key);
            if (k != null)
                return k;
        }
        if (c >= 0)
            return this.key;
        return leftNode.floorKey(key);
    }

    /**
     * Returns the length of the serialized data, including the id and durable length.
     *
     * @return The length of the serialized data.
     */
    public int getDurableLength() {
        if (isNil())
            return 2;
        return 2 + 4 + 4 +
                leftNode.getDurableLength() +
                listNode.getDurableLength() +
                rightNode.getDurableLength() +
                keyFactory.getDurableLength(key);
    }

    /**
     * Serialize this object into a ByteBuffer.
     *
     * @param byteBuffer Where the serialized data is to be placed.
     */
    public void serialize(ByteBuffer byteBuffer) {
        byteBuffer.putInt(level);
        leftNode.writeDurable(byteBuffer);
        listNode.writeDurable(byteBuffer);
        rightNode.writeDurable(byteBuffer);
        keyFactory.writeDurable(key, byteBuffer);
    }

    @Override
    public void releaseAll() {
        if (isNil())
            return;
        if (leftNode instanceof Releasable)
            ((Releasable) leftNode).releaseAll();
        if (listNode instanceof Releasable)
            ((Releasable) listNode).releaseAll();
        if (rightNode instanceof Releasable)
            ((Releasable) rightNode).releaseAll();
    }

    public MapNode replace(int level) {
        thisNode.releaseLocal();
        return new MapNodeImpl(thisNode.getRegistry(), level, leftNode, listNode, rightNode, key);
    }

    public MapNode replace(ListNode listNode) {
        thisNode.releaseLocal();
        return new MapNodeImpl(thisNode.getRegistry(), level, leftNode, listNode, rightNode, key);
    }

    public MapNode replace(int level, ListNode listNode, Comparable key) {
        thisNode.releaseLocal();
        return new MapNodeImpl(thisNode.getRegistry(), level, leftNode, listNode, rightNode, key);
    }

    public MapNode replace(int level, MapNode leftNode, MapNode rightNode) {
        thisNode.releaseLocal();
        return new MapNodeImpl(thisNode.getRegistry(), level, leftNode, listNode, rightNode, key);
    }

    public MapNode replace(MapNode leftNode, ListNode listNode, MapNode rightNode) {
        thisNode.releaseLocal();
        return new MapNodeImpl(thisNode.getRegistry(), level, leftNode, listNode, rightNode, key);
    }

    public MapNode replaceLeft(MapNode leftNode) {
        thisNode.releaseLocal();
        return new MapNodeImpl(thisNode.getRegistry(), level, leftNode, listNode, rightNode, key);
    }

    public MapNode replaceLeft(int level, MapNode leftNode) {
        thisNode.releaseLocal();
        return new MapNodeImpl(thisNode.getRegistry(), level, leftNode, listNode, rightNode, key);
    }

    public MapNode replaceRight(MapNode rightNode) {
        thisNode.releaseLocal();
        return new MapNodeImpl(thisNode.getRegistry(), level, leftNode, listNode, rightNode, key);
    }

    public MapNode replaceRight(int level, MapNode rightNode) {
        thisNode.releaseLocal();
        return new MapNodeImpl(thisNode.getRegistry(), level, leftNode, listNode, rightNode, key);
    }

    @Override
    public Object resize(int maxSize, int maxBlockSize) {
        if (thisNode.getDurableLength() <= maxSize) {
            return thisNode;
        }

        MapNode l = leftNode;
        if (l.getDurableLength() > maxBlockSize)
            l = (MapNode) l.resize(maxBlockSize, maxBlockSize);
        MapNode r = rightNode;
        if (r.getDurableLength() > maxBlockSize)
            r = (MapNode) r.resize(maxBlockSize, maxBlockSize);
        ListNode v = listNode;
        if (v.getDurableLength() > maxBlockSize)
            v = (ListNode) v.resize(maxBlockSize, maxBlockSize);
        if (l != leftNode || r != rightNode || v != listNode)
            return replace(l, v, r).resize(maxSize, maxBlockSize);

        int ldl = leftNode.getDurableLength();
        int vdl = listNode.getDurableLength();
        int rdl = rightNode.getDurableLength();
        Releasable s = leftNode;
        int dl = ldl;
        if (vdl > dl) {
            dl = vdl;
            s = listNode;
        }
        if (rdl > dl) {
            dl = rdl;
            s = rightNode;
        }
        Object q = s.shrink();
        MapNode n;
        if (leftNode == s)
            n = replaceLeft((MapNode) q);
        else if (listNode == s)
            n = replace((ListNode) q);
        else
            n = replaceRight((MapNode) q);
        return n.resize(maxSize, maxBlockSize);
    }

    public String toString() {
        try {
            if (isNil())
                return "";
            return "(" + leftNode.getData().toString() + listNode.getData().toString() + "-" + level + "-" + key + rightNode.getData().toString() + ")";
        } catch (Exception ex) {
            ex.printStackTrace();
            return "?";
        }
    }
}
