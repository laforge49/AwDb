package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;
import java.util.List;

import static java.lang.Math.min;

/**
 * The durable data elements of a list node.
 */
public class ListNodeData implements Releasable {

    /**
     * The node which holds this data.
     */
    public final ListNode thisNode;

    /**
     * Composite node depth--see AA Tree algorithm.
     */
    public final int level;

    /**
     * Number of nodes in this subtree.
     */
    public final int totalSize;

    /**
     * Left subtree node.
     */
    public final ListNode leftNode;

    /**
     * The value of the node.
     */
    public final Object value;

    /**
     * Right subtree node.
     */
    public final ListNode rightNode;

    /**
     * The factory for the value.
     */
    protected final ImmutableFactory valueFactory;

    /**
     * Create the nil node data.
     *
     * @param thisNode The node which holds this data.
     */
    public ListNodeData(ListNode thisNode) {
        this.thisNode = thisNode;
        this.level = 0;
        totalSize = 0;
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
     * @param leftNode  Left subtree node.
     * @param value     The value of the node.
     * @param rightNode Right subtree node.
     */
    public ListNodeData(ListNode thisNode,
                        int level,
                        int totalSize,
                        ListNode leftNode,
                        Object value,
                        ListNode rightNode) {
        this.thisNode = thisNode;
        this.level = level;
        this.totalSize = totalSize;
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
    public ListNodeData(ListNode thisNode, ByteBuffer byteBuffer) {
        this.thisNode = thisNode;
        level = byteBuffer.getInt();
        totalSize = byteBuffer.getInt();
        FactoryRegistry registry = thisNode.getRegistry();
        ImmutableFactory f = registry.readId(byteBuffer);
        leftNode = (ListNode) f.deserialize(byteBuffer);
        valueFactory = registry.readId(byteBuffer);
        value = valueFactory.deserialize(byteBuffer);
        f = registry.readId(byteBuffer);
        rightNode = (ListNode) f.deserialize(byteBuffer);
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
        return 2 + 4 + 4 + 4 +
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
        leftNode.writeDurable(byteBuffer);
        valueFactory.writeDurable(value, byteBuffer);
        rightNode.writeDurable(byteBuffer);
    }

    /**
     * Returns the count of all the values in the list.
     *
     * @return The size of the list.
     */
    public int size() {
        return totalSize;
    }

    /**
     * Returns the selected node.
     *
     * @param ndx Relative position of the selected node within the sublist.
     * @return The selected node, or null.
     */
    public ListNode getListNode(int ndx) {
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
     * Returns the value.
     *
     * @return The value, or null.
     */
    public Object get() {
        return value;
    }

    /**
     * Get the index of a value with the same identity (==).
     * (The list is searched in order.)
     *
     * @param value The value sought.
     * @return The index, or -1.
     */
    public int getIndex(Object value) {
        if (isNil())
            return -1;
        int ndx = leftNode.getIndex(value);
        if (ndx > -1)
            return ndx;
        if (this.value == value)
            return leftNode.totalSize();
        ndx = rightNode.getIndex(value);
        if (ndx == -1)
            return -1;
        return leftNode.totalSize() + 1 + ndx;
    }

    /**
     * Get the index of a value with the same identity (==).
     * (The list is searched in reverse order.)
     *
     * @param value The value sought.
     * @return The index, or -1.
     */
    public int getIndexRight(Object value) {
        if (isNil())
            return -1;
        int ndx = rightNode.getIndexRight(value);
        if (ndx > -1)
            return leftNode.totalSize() + 1 + ndx;
        if (this.value == value)
            return leftNode.totalSize();
        ndx = leftNode.getIndexRight(value);
        if (ndx == -1)
            return -1;
        return ndx;
    }

    /**
     * Find the index of an equal value.
     * (The list is searched in order.)
     *
     * @param value The value sought.
     * @return The index, or -1.
     */
    public int findIndex(Object value) {
        if (isNil())
            return -1;
        int ndx = leftNode.findIndex(value);
        if (ndx > -1)
            return ndx;
        if (this.value.equals(value))
            return leftNode.totalSize();
        ndx = rightNode.findIndex(value);
        if (ndx == -1)
            return -1;
        return leftNode.totalSize() + 1 + ndx;
    }

    /**
     * Find the index of an equal value.
     * (The list is searched in reverse order.)
     *
     * @param value The value sought.
     * @return The index, or -1.
     */
    public int findIndexRight(Object value) {
        if (isNil())
            return -1;
        int ndx = rightNode.findIndexRight(value);
        if (ndx > -1)
            return leftNode.totalSize() + 1 + ndx;
        if (this.value.equals(value))
            return leftNode.totalSize();
        ndx = leftNode.findIndexRight(value);
        if (ndx == -1)
            return -1;
        return ndx;
    }

    /**
     * Appends values to the list.
     *
     * @param list The list being build.
     */
    public void flatList(List list) {
        if (isNil())
            return;
        leftNode.getData().flatList(list);
        list.add(value);
        rightNode.getData().flatList(list);
    }

    /**
     * AA Tree skew operation.
     *
     * @return Revised root node.
     */
    public ListNode skew() {
        if (isNil() || leftNode.isNil())
            return thisNode;
        ListNodeData leftData = leftNode.getData();
        if (leftData.level == level) {
            ListNode t = replaceLeft(
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
    public ListNode split() {
        if (isNil() || rightNode.isNil())
            return thisNode;
        ListNodeData rightData = rightNode.getData();
        if (rightData.rightNode.isNil())
            return thisNode;
        if (level == rightData.rightNode.getData().level) {
            ListNode t = replaceRight(
                    totalSize - rightData.totalSize + rightData.leftNode.totalSize(),
                    rightData.leftNode);
            ListNode r = rightData.replaceLeft(
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
     * @param ndx   Where to add the value, or -1 to append to the end.
     * @param value The value to be added.
     * @return The revised root node.
     */
    public ListNode add(int ndx, Object value) {
        if (ndx == -1)
            ndx = totalSize;
        int leftSize = leftNode.totalSize();
        ListNode t = thisNode;
        if (ndx <= leftSize) {
            t = replaceLeft(totalSize + 1, leftNode.add(ndx, value));
        } else {
            t = replaceRight(totalSize + 1, rightNode.add(ndx - leftSize - 1, value));
        }
        return t.getData().skew().getData().split();
    }

    private ListNode successor() {
        return rightNode.getData().leftMost();
    }

    private ListNode leftMost() {
        if (!leftNode.isNil())
            return leftNode.getData().leftMost();
        return thisNode;
    }

    private ListNode predecessor() {
        return leftNode.getData().rightMost();
    }

    private ListNode rightMost() {
        if (!rightNode.isNil())
            return rightNode.getData().rightMost();
        return thisNode;
    }

    private ListNode decreaseLevel() {
        ListNodeData rd = rightNode.getData();
        int shouldBe = min(leftNode.getData().level, rd.level) + 1;
        if (shouldBe < level) {
            ListNode r;
            if (shouldBe < rd.level)
                r = rd.replace(shouldBe, rd.totalSize);
            else
                r = rightNode;
            return replaceRight(shouldBe, totalSize, r);
        }
        return thisNode;
    }

    public ListNode remove(int ndx) {
        if (isNil())
            return thisNode;
        DbFactoryRegistry registry = thisNode.getRegistry();
        int leftSize = leftNode.size();
        ListNode t = thisNode;
        if (ndx > leftSize) {
            ListNode r = rightNode.remove(ndx - leftSize - 1);
            if (r != rightNode)
                t = replaceRight(totalSize - 1, r);
        } else if (ndx < leftSize) {
            ListNode l = leftNode.remove(ndx);
            if (l != leftNode)
                t = replaceLeft(totalSize - 1, l);
        } else {
            if (value instanceof Releasable)
                ((Releasable) value).releaseAll();
            ListNode nil = registry.nilList;
            if (totalSize == 1) {
                return nil;
            }
            if (leftNode.isNil()) {
                ListNode l = successor();
                t = replaceRight(totalSize - 1, l.getData().value, rightNode.remove(0));
            } else {
                ListNode l = predecessor();
                t = replaceLeft(totalSize - 1, leftNode.remove(leftSize - 1), l.getData().value);
            }
        }
        t = t.getData().decreaseLevel().getData().skew();
        ListNodeData td = t.getData();
        ListNode r = td.rightNode.getData().skew();
        if (!r.isNil()) {
            ListNodeData rd = r.getData();
            ListNode rr = rd.rightNode.getData().skew();
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

    @Override
    public void releaseAll() {
        if (isNil())
            return;
        if (leftNode instanceof Releasable)
            ((Releasable) leftNode).releaseAll();
        if (value instanceof Releasable)
            ((Releasable) value).releaseAll();
        if (rightNode instanceof Releasable)
            ((Releasable) rightNode).releaseAll();
    }

    public ListNode replace(int level, int totalSize) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replace(Object value) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replace(ListNode leftNode, ListNode rightNode) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replace(ListNode leftNode, Object value, ListNode rightNode) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replace(int level, int totalSize, Object value) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replaceLeft(ListNode leftNode) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replaceLeft(int totalSize, ListNode leftNode, Object value) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replaceLeft(int totalSize, ListNode leftNode) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replaceLeft(int level, int totalSize, ListNode leftNode) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replaceRight(ListNode rightNode) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replaceRight(int totalSize, ListNode rightNode) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replaceRight(int totalSize, Object value, ListNode rightNode) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    public ListNode replaceRight(int level, int totalSize, ListNode rightNode) {
        thisNode.releaseLocal();
        return new ListNodeImpl(thisNode.getRegistry(), level, totalSize, leftNode, value, rightNode);
    }

    @Override
    public Object resize(int maxSize, int maxBlockSize) {
        if (thisNode.getDurableLength() <= maxSize) {
            return thisNode;
        }

        ListNode l = leftNode;
        if (l.getDurableLength() > maxBlockSize)
            l = (ListNode) l.resize(maxBlockSize, maxBlockSize);
        ListNode r = rightNode;
        if (r.getDurableLength() > maxBlockSize)
            r = (ListNode) r.resize(maxBlockSize, maxBlockSize);
        Releasable v = null;
        if (value instanceof Releasable) {
            v = (Releasable) value;
            if (v.getDurableLength() > maxBlockSize)
                v = (Releasable) v.resize(maxBlockSize, maxBlockSize);
            if (l != leftNode || r != rightNode || v != value)
                return replace(l, v, r).resize(maxSize, maxBlockSize);
        } else {
            if (l != leftNode || r != rightNode)
                return replace(l, r).resize(maxSize, maxBlockSize);
        }

        int ldl = leftNode.getDurableLength();
        int vdl = v != null ? v.getDurableLength() : -1;
        int rdl = rightNode.getDurableLength();
        Releasable s = leftNode;
        int dl = ldl;
        if (vdl > dl) {
            dl = vdl;
            s = v;
        }
        if (rdl > dl) {
            dl = rdl;
            s = rightNode;
        }
        Object q = s.shrink();
        ListNode n;
        if (leftNode == s)
            n = replaceLeft((ListNode) q);
        else if (value == s)
            n = replace(q);
        else
            n = replaceRight((ListNode) q);
        return n.resize(maxSize, maxBlockSize);
    }

    public String toString() {
        if (isNil())
            return "";
        return "(" + leftNode.getData().toString() + value + "-" + level + "-" + totalSize + rightNode.getData().toString() + ")";
    }
}
