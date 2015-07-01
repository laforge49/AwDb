package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An immutable versioned list.
 */
public interface VersionedListNode extends Releasable {

    /**
     * Returns the database factory registry.
     *
     * @return The registry.
     */
    DbFactoryRegistry getRegistry();

    /**
     * Returns the database.
     *
     * @return The database.
     */
    default Db getDb() {
        return getRegistry().db;
    }

    /**
     * Returns the current timestamp, a unique
     * identifier for the current transaction.
     *
     * @return The current transaction's timestamp
     */
    default long getTimestamp() {
        return getDb().getTimestamp();
    }

    VersionedListNodeData getData();

    /**
     * Returns the count of all the values in the list, deleted or not.
     *
     * @return The count of all the values in the list.
     */
    default int totalSize() {
        return isNil() ? 0 : getData().totalSize;
    }

    default boolean isNil() {
        return this == getRegistry().versionedNilList;
    }

    /**
     * Returns the count of all the values currently in the list.
     *
     * @param timestamp The time of the query.
     * @return The current size of the list.
     */
    default int size(long timestamp) {
        if (isNil())
            return 0;
        return getData().size(timestamp);
    }

    /**
     * Returns a value if it is in range and the value exists for the given time.
     *
     * @param ndx  The index of the selected value.
     * @param timestamp The time of the query.
     * @return A value, or null.
     */
    default Object getExistingValue(int ndx, long timestamp) {
        VersionedListNode n = getData().getListNode(ndx);
        if (n == null)
            return null;
        return n.getData().getExistingValue(timestamp);
    }

    /**
     * Get the index of an existing value with the same identity (==).
     * (The list is searched in order.)
     *
     * @param value The value sought.
     * @param timestamp  The time of the query.
     * @return The index, or -1.
     */
    default int getIndex(Object value, long timestamp) {
        if (isNil())
            return -1;
        return getData().getIndex(value, timestamp);
    }

    /**
     * Get the index of an existing value with the same identity (==).
     * (The list is searched in reverse order.)
     *
     * @param value The value sought.
     * @param timestamp  The time of the query.
     * @return The index, or -1.
     */
    default int getIndexRight(Object value, long timestamp) {
        if (isNil())
            return -1;
        return getData().getIndexRight(value, timestamp);
    }

    /**
     * Find the index of an equal existing value.
     * (The list is searched in order.)
     *
     * @param value The value sought.
     * @param timestamp  The time of the query.
     * @return The index, or -1.
     */
    default int findIndex(Object value, long timestamp) {
        if (isNil())
            return -1;
        return getData().findIndex(value, timestamp);
    }

    /**
     * Find the index of an equal existing value.
     * (The list is searched in reverse order.)
     *
     * @param value The value sought.
     * @param timestamp  The time of the query.
     * @return The index, or -1.
     */
    default int findIndexRight(Object value, long timestamp) {
        if (isNil())
            return -1;
        return getData().findIndexRight(value, timestamp);
    }

    /**
     * Returns the index of an existing value higher than the given index.
     *
     * @param ndx  A given index.
     * @param timestamp The time of the query.
     * @return An index of an existing value that is higher, or -1.
     */
    default int higherIndex(int ndx, long timestamp) {
        return getData().higherIndex(ndx, timestamp);
    }

    /**
     * Returns the index of an existing value higher than or equal to the given index.
     *
     * @param ndx  A given index.
     * @param timestamp The time of the query.
     * @return An index of an existing value that is higher or equal, or -1.
     */
    default int ceilingIndex(int ndx, long timestamp) {
        return getData().ceilingIndex(ndx, timestamp);
    }

    /**
     * Returns the index of the first existing value in the list.
     *
     * @param timestamp The time of the query.
     * @return The index of the first existing value in the list, or -1.
     */
    default int firstIndex(long timestamp) {
        return ceilingIndex(0, timestamp);
    }

    /**
     * Returns the index of an existing value lower than the given index.
     *
     * @param ndx  A given index.
     * @param timestamp The time of the query.
     * @return An index of an existing value that is lower, or -1.
     */
    default int lowerIndex(int ndx, long timestamp) {
        if (ndx <= 0 || isNil())
            return -1; //out of range
        return getData().lowerIndex(ndx, timestamp);
    }

    /**
     * Returns the index of an existing value lower than or equal to the given index.
     *
     * @param ndx  A given index.
     * @param timestamp The time of the query.
     * @return An index of an existing value that is lower or equal, or -1.
     */
    default int floorIndex(int ndx, long timestamp) {
        if (ndx < 0 || isNil())
            return -1; //out of range
        return getData().floorIndex(ndx, timestamp);
    }

    /**
     * Returns the index of the last existing value in the list.
     *
     * @param timestamp The time of the query.
     * @return The index of the last existing value in the list, or -1.
     */
    default int lastIndex(long timestamp) {
        return floorIndex(totalSize(), timestamp);
    }

    /**
     * Returns true if there are no values present for the given time.
     *
     * @param timestamp The time of the query.
     * @return Returns true if the list is empty for the given time.
     */
    default boolean isEmpty(long timestamp) {
        if (isNil())
            return true;
        return getData().isEmpty(timestamp);
    }

    /**
     * Returns a list of all the values that are present for a given time.
     *
     * @param timestamp The time of the query.
     * @return A list of all values present for the given time.
     */
    default List flatList(long timestamp) {
        List list = new ArrayList<>();
        getData().flatList(list, timestamp);
        return list;
    }

    /**
     * Returns an iterator over the existing values.
     *
     * @param timestamp The time of the query.
     * @return The iterator.
     */
    default PeekABoo iterator(long timestamp) {
        return new PeekABoo() {
            int next = ceilingIndex(0, timestamp);

            @Override
            public String getPosition() {
                return "" + next;
            }

            @Override
            public void setPosition(String state) {
                next = ceilingIndex(Integer.parseInt(state), timestamp);
            }

            @Override
            public boolean positionPrior() {
                int n;
                if (next == -1)
                    n = lastIndex(timestamp);
                else
                    n = lowerIndex(next, timestamp);
                if (n == -1)
                    return false;
                next = n;
                return true;
            }

            @Override
            public Object peek() {
                return getExistingValue(next, timestamp);
            }

            @Override
            public boolean hasNext() {
                return next > -1;
            }

            @Override
            public Object next() {
                if (next == -1)
                    throw new NoSuchElementException();
                int rv = next;
                next = higherIndex(next, timestamp);
                return getExistingValue(rv, timestamp);
            }
        };
    }

    /**
     * Returns a list accessor for the time of the current transaction.
     *
     * @return A list accessor for the current time.
     */
    default ListAccessor listAccessor() {
        return listAccessor(null, getTimestamp());
    }

    /**
     * Returns a list accessor for the time of the current transaction.
     *
     * @param key The key for the list.
     * @return A list accessor for the latest time.
     */
    default ListAccessor listAccessor(Comparable key) {
        return listAccessor(key, getTimestamp());
    }

    /**
     * Returns a list accessor for the given time.
     *
     * @param key  The key for the list.
     * @param timestamp The time of the query.
     * @return A list accessor for the given time.
     */
    default ListAccessor listAccessor(Comparable key, long timestamp) {
        return new ListAccessor() {
            @Override
            public Comparable key() {
                return key;
            }

            @Override
            public long getTimestamp() {
                return timestamp;
            }

            @Override
            public int size() {
                return VersionedListNode.this.size(timestamp);
            }

            @Override
            public Object get(int ndx) {
                return VersionedListNode.this.getExistingValue(ndx, timestamp);
            }

            @Override
            public int getIndex(Object value) {
                return VersionedListNode.this.getIndex(value, timestamp);
            }

            @Override
            public int getIndexRight(Object value) {
                return VersionedListNode.this.getIndexRight(value, timestamp);
            }

            @Override
            public int findIndex(Object value) {
                return VersionedListNode.this.findIndex(value, timestamp);
            }

            @Override
            public int findIndexRight(Object value) {
                return VersionedListNode.this.findIndexRight(value, timestamp);
            }

            @Override
            public int higherIndex(int ndx) {
                return VersionedListNode.this.higherIndex(ndx, timestamp);
            }

            @Override
            public int ceilingIndex(int ndx) {
                return VersionedListNode.this.ceilingIndex(ndx, timestamp);
            }

            @Override
            public int firstIndex() {
                return VersionedListNode.this.firstIndex(timestamp);
            }

            @Override
            public int lowerIndex(int ndx) {
                return VersionedListNode.this.lowerIndex(ndx, timestamp);
            }

            @Override
            public int floorIndex(int ndx) {
                return VersionedListNode.this.floorIndex(ndx, timestamp);
            }

            @Override
            public int lastIndex() {
                return VersionedListNode.this.lastIndex(timestamp);
            }

            @Override
            public boolean isEmpty() {
                return VersionedListNode.this.isEmpty(timestamp);
            }

            @Override
            public List flatList() {
                return VersionedListNode.this.flatList(timestamp);
            }

            @Override
            public PeekABoo iterator() {
                return VersionedListNode.this.iterator(timestamp);
            }
        };
    }

    /**
     * Add a non-null value to the end of the list.
     *
     * @param value The value to be added.
     * @return The revised root node.
     */
    default VersionedListNode add(Object value) {
        return add(-1, value);
    }

    /**
     * Add a non-null value to the list.
     *
     * @param ndx   Where to add the value, or -1 to append to the end.
     * @param value The value to be added.
     * @return The revised root node.
     */
    default VersionedListNode add(int ndx, Object value) {
        return add(ndx, value, getTimestamp(), Long.MAX_VALUE);
    }

    default VersionedListNode add(int ndx, Object value, long created, long deleted) {
        if (value == null)
            throw new IllegalArgumentException("value may not be null");
        if (isNil()) {
            if (ndx != 0 && ndx != -1)
                throw new IllegalArgumentException("index out of range");
            return getData().replace(
                    1,
                    1,
                    created,
                    deleted,
                    value);
        }
        return getData().add(ndx, value, created, deleted);
    }

    /**
     * Mark a value as deleted.
     *
     * @param ndx  The index of the value.
     * @return The revised node.
     */
    default VersionedListNode remove(int ndx) {
        if (isNil())
            return this;
        return getData().remove(ndx);
    }

    /**
     * Mark the first occurance of a value in a list as deleted.
     *
     * @param x    The value to be removed.
     * @return The updated root.
     */
    default VersionedListNode remove(Object x) {
        if (isNil())
            return this;
        int ts = totalSize();
        for (int i = 0; i < ts; ++i) {
            Object y = getExistingValue(i, getTimestamp());
            if (y != null && x.equals(y)) {
                VersionedListNode ln = remove(i);
                return ln;
            }
        }
        return this;
    }

    /**
     * Perform a complete copy.
     *
     * @return A complete, but shallow copy of the list.
     */
    default VersionedListNode copyList() {
        return copyList(0L);
    }

    /**
     * Copy everything except what was deleted before a given time.
     * (This is a shallow copy, as the values in the list are not copied.)
     *
     * @param timestamp The given time.
     * @return A shortened copy of the list without some historical values.
     */
    default VersionedListNode copyList(long timestamp) {
        return getData().copyList(getRegistry().versionedNilList, timestamp);
    }

    /**
     * Empty the list by marking all the existing values as deleted.
     *
     * @return The currently empty versioned list.
     */
    default VersionedListNode clearList() {
        return getData().clearList();
    }

    /**
     * Returns the size of a byte array needed to serialize this object,
     * including the space needed for the durable id.
     *
     * @return The size in bytes of the serialized data.
     */
    int getDurableLength();

    /**
     * Write the durable to a byte buffer.
     *
     * @param byteBuffer The byte buffer.
     */
    void writeDurable(ByteBuffer byteBuffer);

    /**
     * Serialize this object into a ByteBuffer.
     *
     * @param byteBuffer Where the serialized data is to be placed.
     */
    void serialize(ByteBuffer byteBuffer);

    @Override
    default void releaseAll() {
        if (isNil())
            return;
        getData().releaseAll();
    }

    @Override
    default Object resize(int maxSize, int maxBlockSize) {
        return getData().resize(maxSize, maxBlockSize);
    }

    /**
     * Returns a ByteBuffer loaded with the serialized contents of the immutable.
     *
     * @return The loaded ByteBuffer.
     */
    default ByteBuffer toByteBuffer() {
        ImmutableFactory factory = getRegistry().getImmutableFactory(this);
        return factory.toByteBuffer(this);
    }
}
