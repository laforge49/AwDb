package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * The durable data elements of a versioned list node.
 */
public class VersionedListNodeData implements Releasable {

    /**
     * The node which holds this data.
     */
    public final VersionedListNode thisNode;

    /**
     * Composite node depth--see AA Tree algorithm.
     */
    public final int level;

    /**
     * Number of nodes in this subtree.
     */
    public final int totalSize;

    /**
     * Creation time of this node.
     */
    public final long created;

    /**
     * Deletion time of this node.
     */
    public final long deleted;

    /**
     * Left subtree node.
     */
    public final VersionedListNode leftNode;

    /**
     * The value of the node.
     */
    public final Object value;

    /**
     * Right subtree node.
     */
    public final VersionedListNode rightNode;

    /**
     * The factory for the value.
     */
    protected final ImmutableFactory valueFactory;

    /**
     * Create the nil node data.
     *
     * @param thisNode The node which holds this data.
     */
    public VersionedListNodeData(VersionedListNode thisNode) {
        this.thisNode = thisNode;
        this.level = 0;
        totalSize = 0;
        this.created = 0L;
        this.deleted = 0L;
        this.leftNode = thisNode;
        this.value = null;
        this.rightNode = thisNode;

        valueFactory = null;
    }

    /**
     * Create non-nill node data.
     *
     * @param thisNode  The node which holds this data.
     * @param level     Composite node depth--see AA Tree algorithm.
     * @param totalSize Number of nodes in this subtree.
     * @param created   Creation time of this node.
     * @param deleted   Deletion time of this node.
     * @param leftNode  Left subtree node.
     * @param value     The value of the node.
     * @param rightNode Right subtree node.
     */
    public VersionedListNodeData(VersionedListNode thisNode,
                                 int level,
                                 int totalSize,
                                 long created,
                                 long deleted,
                                 VersionedListNode leftNode,
                                 Object value,
                                 VersionedListNode rightNode) {
        this.thisNode = thisNode;
        this.level = level;
        this.totalSize = totalSize;
        this.created = created;
        this.deleted = deleted;
        this.leftNode = leftNode;
        this.value = value;
        this.rightNode = rightNode;
        this.valueFactory = thisNode.getRegistry().getImmutableFactory(value);
    }

    /**
     * Create non-nill node data.
     *
     * @param thisNode   The node which holds this data.
     * @param byteBuffer Holds the serialized data.
     */
    public VersionedListNodeData(VersionedListNode thisNode, ByteBuffer byteBuffer) {
        this.thisNode = thisNode;
        level = byteBuffer.getInt();
        totalSize = byteBuffer.getInt();
        created = byteBuffer.getLong();
        deleted = byteBuffer.getLong();
        FactoryRegistry registry = thisNode.getRegistry();
        ImmutableFactory f = registry.readId(byteBuffer);
        leftNode = (VersionedListNode) f.deserialize(byteBuffer);
        valueFactory = registry.readId(byteBuffer);
        value = valueFactory.deserialize(byteBuffer);
        f = registry.readId(byteBuffer);
        rightNode = (VersionedListNode) f.deserialize(byteBuffer);
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
     * Returns the length of the serialized data, including the id and durable length.
     *
     * @return The length of the serialized data.
     */
    public int getDurableLength() {
        if (isNil())
            return 2;
        return 2 + 4 + 4 + 4 + 8 + 8 +
                leftNode.getDurableLength() +
                valueFactory.getDurableLength(value) +
                rightNode.getDurableLength();
    }

    /**
     * Serialize this object into a ByteBuffer.
     *
     * @param byteBuffer Where the serialized data is to be placed.
     */
    public void serialize(ByteBuffer byteBuffer) {
        byteBuffer.putInt(level);
        byteBuffer.putInt(totalSize);
        byteBuffer.putLong(created);
        byteBuffer.putLong(deleted);
        leftNode.writeDurable(byteBuffer);
        valueFactory.writeDurable(value, byteBuffer);
        rightNode.writeDurable(byteBuffer);
    }

    /**
     * Returns true if the value of the node exists for the given time.
     *
     * @param timestamp The time of the query.
     * @return True if the value currently exists.
     */
    public boolean exists(long timestamp) {
        return timestamp >= created && timestamp < deleted;
    }

    /**
     * Returns the count of all the values currently in the list.
     *
     * @param timestamp The time of the query.
     * @return The current size of the list.
     */
    public int size(long timestamp) {
        if (isNil())
            return 0;
        int s = leftNode.size(timestamp) + rightNode.size(timestamp);
        if (exists(timestamp))
            s += 1;
        return s;
    }

    /**
     * Returns the selected node.
     *
     * @param ndx Relative position of the selected node within the sublist.
     * @return The selected node, or null.
     */
    public VersionedListNode getListNode(int ndx) {
        if (ndx < 0 || ndx >= totalSize)
            return null; //out of range
        int leftSize = leftNode.totalSize();
        if (ndx < leftSize)
            return leftNode.getData().getListNode(ndx);
        if (ndx > leftSize)
            return rightNode.getData().getListNode(ndx - leftSize - 1);
        return thisNode;
    }

    /**
     * Returns the value if it exists.
     *
     * @param timestamp The time of the query.
     * @return The value, or null.
     */
    public Object getExistingValue(long timestamp) {
        return exists(timestamp) ? value : null;
    }

    /**
     * Get the index of an existing value with the same identity (==).
     * (The list is searched in order.)
     *
     * @param value The value sought.
     * @param timestamp  The time of the query.
     * @return The index, or -1.
     */
    public int getIndex(Object value, long timestamp) {
        if (isNil())
            return -1;
        int ndx = leftNode.getIndex(value, timestamp);
        if (ndx > -1)
            return ndx;
        if (this.value == value && exists(timestamp))
            return leftNode.totalSize();
        ndx = rightNode.getIndex(value, timestamp);
        if (ndx == -1)
            return -1;
        return leftNode.totalSize() + 1 + ndx;
    }

    /**
     * Get the index of an existing value with the same identity (==).
     * (The list is searched in reverse order.)
     *
     * @param value The value sought.
     * @param timestamp  The time of the query.
     * @return The index, or -1.
     */
    public int getIndexRight(Object value, long timestamp) {
        if (isNil())
            return -1;
        int ndx = rightNode.getIndexRight(value, timestamp);
        if (ndx > -1)
            return leftNode.totalSize() + 1 + ndx;
        if (this.value == value && exists(timestamp))
            return leftNode.totalSize();
        ndx = leftNode.getIndexRight(value, timestamp);
        if (ndx == -1)
            return -1;
        return ndx;
    }

    /**
     * Find the index of an equal existing value.
     * (The list is searched in order.)
     *
     * @param value The value sought.
     * @param timestamp  The time of the query.
     * @return The index, or -1.
     */
    public int findIndex(Object value, long timestamp) {
        if (isNil())
            return -1;
        int ndx = leftNode.findIndex(value, timestamp);
        if (ndx > -1)
            return ndx;
        if (exists(timestamp) && this.value.equals(value))
            return leftNode.totalSize();
        ndx = rightNode.findIndex(value, timestamp);
        if (ndx == -1)
            return -1;
        return leftNode.totalSize() + 1 + ndx;
    }

    /**
     * Find the index of an equal existing value.
     * (The list is searched in reverse order.)
     *
     * @param value The value sought.
     * @param timestamp  The time of the query.
     * @return The index, or -1.
     */
    public int findIndexRight(Object value, long timestamp) {
        if (isNil())
            return -1;
        int ndx = rightNode.findIndexRight(value, timestamp);
        if (ndx > -1)
            return leftNode.totalSize() + 1 + ndx;
        if (exists(timestamp) && this.value.equals(value))
            return leftNode.totalSize();
        ndx = leftNode.findIndexRight(value, timestamp);
        if (ndx == -1)
            return -1;
        return ndx;
    }

    /**
     * Returns the index of an existing value higher than the given index.
     *
     * @param ndx  A given index.
     * @param timestamp The time of the query.
     * @return An index of an existing value that is higher, or -1.
     */
    public int higherIndex(int ndx, long timestamp) {
        if (ndx >= totalSize - 1 || isNil())
            return -1; //out of range
        int leftSize = leftNode.totalSize();
        if (ndx < leftSize - 1) {
            int h = leftNode.higherIndex(ndx, timestamp);
            if (h > -1)
                return h;
        }
        if (ndx < leftSize) {
            if (exists(timestamp))
                return leftSize;
        }
        int h = rightNode.higherIndex(ndx - leftSize - 1, timestamp);
        return h == -1 ? -1 : h + leftSize + 1;
    }

    /**
     * Returns the index of an existing value higher than or equal to the given index.
     *
     * @param ndx  A given index.
     * @param timestamp The time of the query.
     * @return An index of an existing value that is higher or equal, or -1.
     */
    public int ceilingIndex(int ndx, long timestamp) {
        if (ndx >= totalSize || isNil()) {
            return -1; //out of range
        }
        int leftSize = leftNode.totalSize();
        if (ndx < leftSize) {
            int h = leftNode.ceilingIndex(ndx, timestamp);
            if (h > -1)
                return h;
        }
        if (ndx <= leftSize) {
            if (exists(timestamp))
                return leftSize;
        }
        int h = rightNode.ceilingIndex(ndx - leftSize - 1, timestamp);
        return h <= -1 ? -1 : h + leftSize + 1;
    }

    /**
     * Returns the index of an existing value lower than the given index.
     *
     * @param ndx  A given index.
     * @param timestamp The time of the query.
     * @return An index of an existing value that is lower, or -1.
     */
    public int lowerIndex(int ndx, long timestamp) {
        if (ndx <= 0 || isNil())
            return -1; //out of range
        int leftSize = leftNode.totalSize();
        if (ndx > leftSize + 1) {
            int l = rightNode.lowerIndex(ndx - leftSize - 1, timestamp);
            if (l > -1)
                return l + leftSize + 1;
        }
        if (ndx > leftSize) {
            if (exists(timestamp))
                return leftSize;
        }
        return leftNode.lowerIndex(ndx, timestamp);
    }

    /**
     * Returns the index of an existing value lower than or equal to the given index.
     *
     * @param ndx  A given index.
     * @param timestamp The time of the query.
     * @return An index of an existing value that is lower or equal, or -1.
     */
    public int floorIndex(int ndx, long timestamp) {
        if (ndx < 0 || isNil())
            return -1; //out of range
        int leftSize = leftNode.totalSize();
        if (ndx > leftSize) {
            int l = rightNode.floorIndex(ndx - leftSize - 1, timestamp);
            if (l > -1)
                return l + leftSize + 1;
        }
        if (ndx >= leftSize) {
            if (exists(timestamp))
                return leftSize;
        }
        return leftNode.floorIndex(ndx, timestamp);
    }

    /**
     * Returns true if there are no values present for the given time.
     *
     * @param timestamp The time of the query.
     * @return Returns true if the list is empty for the given time.
     */
    public boolean isEmpty(long timestamp) {
        if (isNil())
            return true;
        return (!exists(timestamp) && leftNode.isEmpty(timestamp) && rightNode.isEmpty(timestamp));
    }

    /**
     * Appends existing values to the list.
     *
     * @param list The list being build.
     * @param timestamp The time of the query.
     */
    public void flatList(List list, long timestamp) {
        if (isNil())
            return;
        leftNode.getData().flatList(list, timestamp);
        if (exists(timestamp))
            list.add(value);
        rightNode.getData().flatList(list, timestamp);
    }

    /**
     * AA Tree skew operation.
     *
     * @return Revised root node.
     */
    public VersionedListNode skew() {
        if (isNil() || leftNode.isNil())
            return thisNode;
        VersionedListNodeData leftData = leftNode.getData();
        if (leftData.level == level) {
            VersionedListNode t = replaceLeft(
                    totalSize - leftData.totalSize + leftData.rightNode.totalSize(),
                    leftData.rightNode);
            return leftData.replaceRight(totalSize, t);
        } else
            return thisNode;
    }

    /**
     * AA Tree split
     *
     * @return The revised root node.
     */
    public VersionedListNode split() {
        if (isNil() || rightNode.isNil())
            return thisNode;
        VersionedListNodeData rightData = rightNode.getData();
        if (rightData.rightNode.isNil())
            return thisNode;
        if (level == rightData.rightNode.getData().level) {
            VersionedListNode t = replaceRight(
                    totalSize - rightData.totalSize + rightData.leftNode.totalSize(),
                    rightData.leftNode);
            VersionedListNode r = rightData.replaceLeft(
                    rightData.level + 1,
                    totalSize,
                    t);
            return r;
        }
        return thisNode;
    }

    /**
     * Add a non-null value to the list.
     *
     * @param ndx     Where to add the value, or -1 to append to the end.
     * @param value   The value to be added.
     * @param created Creation time.
     * @param deleted Deletion time, or MAX_VALUE.
     * @return The revised root node.
     */
    public VersionedListNode add(int ndx, Object value, long created, long deleted) {
        if (ndx == -1)
            ndx = totalSize;
        int leftSize = leftNode.totalSize();
        VersionedListNode t = thisNode;
        if (ndx <= leftSize) {
            t = replaceLeft(
                    totalSize + 1,
                    leftNode.add(ndx, value, created, deleted));
        } else {
            t = replaceRight(
                    totalSize + 1,
                    rightNode.add(ndx - leftSize - 1, value, created, deleted));
        }
        return t.getData().skew().getData().split();
    }

    /**
     * Mark a value as deleted.
     *
     * @param ndx  The index of the value.
     * @return The revised node.
     */
    public VersionedListNode remove(int ndx) {
        if (isNil())
            return thisNode;
        long time = thisNode.getTimestamp();
        int leftSize = leftNode.totalSize();
        if (ndx == leftSize) {
            if (exists(time)) {
                VersionedListNode ln = replace(time);
                return ln;
            }
            return thisNode;
        }
        if (ndx < leftSize) {
            VersionedListNode n = leftNode.remove(ndx);
            if (leftNode == n)
                return thisNode;
            return replaceLeft(n);
        }
        VersionedListNode n = rightNode.remove(ndx - leftSize - 1);
        if (rightNode == n)
            return thisNode;
        return replaceRight(n);
    }

    /**
     * Copy everything except what was deleted before a given time.
     * (This is a shallow copy, as the values in the list are not copied.)
     *
     * @param n    The new list.
     * @param timestamp The given time.
     * @return A shortened copy of the list without some historical values.
     */
    public VersionedListNode copyList(VersionedListNode n, long timestamp) {
        if (isNil())
            return n;
        n = leftNode.getData().copyList(n, timestamp);
        if (deleted >= timestamp)
            n = n.add(n.totalSize(), value, created, deleted);
        return rightNode.getData().copyList(n, timestamp);
    }

    /**
     * Empty the list by marking all the existing values as deleted.
     *
     * @return The currently empty versioned list.
     */
    public VersionedListNode clearList() {
        if (isNil())
            return thisNode;
        VersionedListNode ln = leftNode.clearList();
        VersionedListNode rn = rightNode.clearList();
        if (ln == leftNode && rn == rightNode && !exists(thisNode.getTimestamp()))
            return thisNode;
        return replace(thisNode.getTimestamp(), ln, rn);
    }

    @Override
    public void releaseAll() {
        if (leftNode instanceof Releasable)
            ((Releasable) leftNode).releaseAll();
        if (value instanceof Releasable)
            ((Releasable) value).releaseAll();
        if (rightNode instanceof Releasable)
            ((Releasable) rightNode).releaseAll();
    }

    public VersionedListNode replace(long deleted) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replace(int level, int totalSize, long created, long deleted, Object value) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replace(VersionedListNode leftNode, VersionedListNode rightNode) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replace(long deleted, VersionedListNode leftNode, VersionedListNode rightNode) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replaceLeft(int totalSize, VersionedListNode leftNode, Object value) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replaceLeft(VersionedListNode leftNode) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replaceLeft(int totalSize, VersionedListNode leftNode) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replaceLeft(int level, int totalSize, VersionedListNode leftNode) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replaceRight(VersionedListNode rightNode) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replaceRight(int totalSize, VersionedListNode rightNode) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replaceRight(int totalSize, Object value, VersionedListNode rightNode) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    public VersionedListNode replaceRight(int level, int totalSize, VersionedListNode rightNode) {
        thisNode.releaseLocal();
        return new VersionedListNodeImpl(thisNode.getRegistry(), level, totalSize, created, deleted, leftNode, value, rightNode);
    }

    @Override
    public Object resize(int maxSize, int maxBlockSize) {
        if (thisNode.getDurableLength() <= maxSize) {
            return thisNode;
        }

        VersionedListNode l = leftNode;
        if (l.getDurableLength() > maxBlockSize)
            l = (VersionedListNode) l.resize(maxBlockSize, maxBlockSize);
        VersionedListNode r = rightNode;
        if (r.getDurableLength() > maxBlockSize)
            r = (VersionedListNode) r.resize(maxBlockSize, maxBlockSize);
        if (l != leftNode || r != rightNode)
            return replace(l, r).resize(maxSize, maxBlockSize);

        int ldl = leftNode.getDurableLength();
        int rdl = rightNode.getDurableLength();
        Releasable s = leftNode;
        int dl = ldl;
        if (rdl > dl) {
            dl = rdl;
            s = rightNode;
        }
        Object q = s.shrink();
        VersionedListNode n;
        if (leftNode == s)
            n = replaceLeft((VersionedListNode) q);
        else
            n = replaceRight((VersionedListNode) q);
        return n.resize(maxSize, maxBlockSize);
    }

    public String toString() {
        if (isNil())
            return "";
        return "(" + leftNode.getData().toString() + value + "-" + level + "-" + totalSize + rightNode.getData().toString() + ")";
    }
}
