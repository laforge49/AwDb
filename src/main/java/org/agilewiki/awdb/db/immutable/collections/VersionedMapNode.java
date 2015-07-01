package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * An immutable map of versioned lists.
 */
public interface VersionedMapNode extends Releasable {

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

    VersionedMapNodeData getData();

    default boolean isNil() {
        return this == getRegistry().versionedNilMap;
    }

    /**
     * Returns the list for the node.
     *
     * @param key The key for the node.
     * @return The list, or null.
     */
    default VersionedListNode getList(Comparable key) {
        if (isNil())
            return getRegistry().versionedNilList;
        return getData().getList(key);
    }

    /**
     * Returns the selected object.
     *
     * @param key       The key of the selected object.
     * @param timestamp The time of the query.
     * @return The object, or null.
     */
    default Object get(Comparable key, long timestamp) {
        VersionedListNode ln = getList(key);
        if (ln == null) {
            return null;
        }
        if (ln.isEmpty(timestamp))
            return null;
        int i = ln.firstIndex(timestamp);
        if (i == -1)
            return null;
        return ln.getExistingValue(i, timestamp);
    }

    /**
     * Returns the count of all the values in the list,
     * including deleted values.
     *
     * @param key The list identifier.
     * @return The count of all the values in the list.
     */
    default int totalSize(Comparable key) {
        return getList(key).totalSize();
    }

    /**
     * Returns a list accessor for the latest time.
     *
     * @param key The key for the list.
     * @return A list accessor for the latest time, or null.
     */
    default ListAccessor listAccessor(Comparable key) {
        VersionedListNode vln = getList(key);
        if (vln == null)
            return null;
        return vln.listAccessor(key);
    }

    /**
     * Returns a list accessor for the given time.
     *
     * @param key       The key for the list.
     * @param timestamp The time of the query.
     * @return A list accessor for the given time, or null.
     */
    default ListAccessor listAccessor(Comparable key, long timestamp) {
        VersionedListNode vln = getList(key);
        if (vln == null)
            return null;
        return vln.listAccessor(key, timestamp);
    }

    /**
     * Add a non-null value to the end of the list.
     *
     * @param key   The key of the list.
     * @param value The value to be added.
     * @return The revised root node.
     */
    default VersionedMapNode add(Comparable key, Object value) {
        return add(key, -1, value);
    }

    /**
     * Add a non-null value to the list.
     *
     * @param key   The key of the list.
     * @param ndx   Where to add the value.
     * @param value The value to be added.
     * @return The revised root node.
     */
    default VersionedMapNode add(Comparable key, int ndx, Object value) {
        return add(key, ndx, value, getTimestamp(), Long.MAX_VALUE);
    }

    default VersionedMapNode add(Comparable key, int ndx, Object value, long created, long deleted) {
        if (key == null)
            throw new IllegalArgumentException("key may not be null");
        if (isNil()) {
            DbFactoryRegistry registry = getRegistry();
            VersionedListNode listNode = registry.versionedNilList.add(ndx, value, created, deleted);
            return getData().replace(1, listNode, key);
        }
        return getData().add(key, ndx, value, created, deleted);
    }

    /**
     * Mark a value as deleted.
     *
     * @param key The key of the list.
     * @param ndx The index of the value.
     * @return The revised node.
     */
    default VersionedMapNode remove(Comparable key, int ndx) {
        if (isNil())
            return this;
        return getData().remove(key, ndx);
    }

    /**
     * Remove the first occurance of a value from a list.
     *
     * @param key The key of the list.
     * @param x   The value to be removed.
     * @return The updated root.
     */
    default VersionedMapNode remove(Comparable key, Object x) {
        if (isNil())
            return this;
        VersionedMapNode n = getData().remove(key, x);
        return n;
    }

    /**
     * Empty the list by marking all the existing values as deleted.
     *
     * @param key The key of the list.
     * @return The revised node.
     */
    default VersionedMapNode clearList(Comparable key) {
        if (isNil())
            return this;
        return getData().clearList(key);
    }

    /**
     * Replace the list entries with a single value.
     *
     * @param key   The key of the list.
     * @param value The new value.
     * @return The revised node.
     */
    default VersionedMapNode set(Comparable key, Object value) {
        if (value == null)
            throw new IllegalArgumentException("value may not be null");
        if (isNil()) {
            DbFactoryRegistry registry = getRegistry();
            VersionedListNode listNode = registry.versionedNilList.add(value);
            return getData().replace(1, listNode, key);
        }
        return getData().set(key, value);
    }

    /**
     * Empty the map by marking all the existing values as deleted.
     *
     * @return The currently empty versioned map.
     */
    default VersionedMapNode clearMap() {
        if (isNil())
            return this;
        return getData().clearMap();
    }

    /**
     * Perform a complete list copy.
     *
     * @return A complete, but shallow copy of the list.
     */
    default VersionedListNode copyList(Comparable key) {
        return getList(key).copyList();
    }

    /**
     * Copy everything in the list except what was deleted before a given time.
     * (This is a shallow copy, as the values in the list are not copied.)
     *
     * @param timestamp The given time.
     * @return A shortened copy of the list without some historical values.
     */
    default VersionedListNode copyList(Comparable key, long timestamp) {
        return getList(key).copyList(timestamp);
    }

    /**
     * Returns a set of all keys with non-empty lists for the given time.
     *
     * @param timestamp The time of the query.
     * @return A set of the keys with content at the time of the query.
     */
    default NavigableSet<Comparable> flatKeys(long timestamp) {
        NavigableSet keys = new TreeSet<>();
        getData().flatKeys(keys, timestamp);
        return keys;
    }

    /**
     * Returns a map of all the keys and values present at the given time.
     *
     * @param timestamp The time of the query.
     * @return A map of lists.
     */
    default NavigableMap<Comparable, List> flatMap(long timestamp) {
        NavigableMap<Comparable, List> map = new TreeMap<Comparable, List>();
        getData().flatMap(map, timestamp);
        return map;
    }

    /**
     * Perform a complete copy.
     *
     * @return A complete, but shallow copy of the list.
     */
    default VersionedMapNode copyMap() {
        return copyMap(0L);
    }

    /**
     * Copy everything except what was deleted before a given time.
     * (This is a shallow copy, as the values in the lists are not copied.)
     *
     * @param timestamp The given time.
     * @return A shortened copy of the map without some historical values.
     */
    default VersionedMapNode copyMap(long timestamp) {
        return getData().copyMap(getRegistry().versionedNilMap, timestamp);
    }

    /**
     * Returns the count of all the keys in the map, empty or not.
     *
     * @return The count of all the keys in the map.
     */
    default int totalSize() {
        if (isNil())
            return 0;
        return getData().totalSize();
    }

    /**
     * Returns the count of all the keys with a non-empty list.
     *
     * @param timestamp The time of the query.
     * @return The current size of the map.
     */
    default int size(long timestamp) {
        if (isNil())
            return 0;
        return getData().size(timestamp);
    }

    /**
     * Returns true if all lists are empty.
     *
     * @param timestamp The time of the query.
     * @return False if there is any content.
     */
    default boolean isEmpty(long timestamp) {
        if (isNil())
            return true;
        return getData().isEmpty(timestamp);
    }

    /**
     * Returns the smallest key of the non-empty lists for the given time.
     *
     * @param timestamp The time of the query.
     * @return The smallest key, or null.
     */
    default Comparable firstKey(long timestamp) {
        if (isNil())
            return null;
        return getData().firstKey(timestamp);
    }

    /**
     * Returns the largest key of the non-empty lists for the given time.
     *
     * @param timestamp The time of the query.
     * @return The largest key, or null.
     */
    default Comparable lastKey(long timestamp) {
        if (isNil())
            return null;
        return getData().lastKey(timestamp);
    }

    /**
     * Returns the next greater key.
     *
     * @param key       The given key.
     * @param timestamp The time of the query.
     * @return The next greater key with content at the time of the query, or null.
     */
    default Comparable higherKey(Comparable key, long timestamp) {
        if (isNil())
            return null;
        return getData().higherKey(key, timestamp);
    }

    /**
     * Returns the key with content that is greater than or equal to the given key.
     *
     * @param key       The given key.
     * @param timestamp The time of the query.
     * @return The key greater than or equal to the given key, or null.
     */
    default Comparable ceilingKey(Comparable key, long timestamp) {
        if (isNil())
            return null;
        return getData().ceilingKey(key, timestamp);
    }

    /**
     * Returns the next smaller key.
     *
     * @param key       The given key.
     * @param timestamp The time of the query.
     * @return The next smaller key with content at the time of the query, or null.
     */
    default Comparable lowerKey(Comparable key, long timestamp) {
        if (isNil())
            return null;
        return getData().lowerKey(key, timestamp);
    }

    /**
     * Returns the key with content that is smaller than or equal to the given key.
     *
     * @param key       The given key.
     * @param timestamp The time of the query.
     * @return The key smaller than or equal to the given key, or null.
     */
    default Comparable floorKey(Comparable key, long timestamp) {
        if (isNil())
            return null;
        return getData().floorKey(key, timestamp);
    }

    /**
     * Returns an iterator over the non-empty list accessors.
     *
     * @param timestamp The time of the query.
     * @return The iterator.
     */
    default PeekABoo<ListAccessor> iterator(long timestamp) {
        return new PeekABoo<ListAccessor>() {

            String next = (String) firstKey(timestamp);

            @Override
            public String getPosition() {
                return next;
            }

            @Override
            public void setPosition(String state) {
                next = (String) ceilingKey(state, timestamp);
            }

            @Override
            public boolean positionPrior() {
                if (next == null) {
                    next = (String) lastKey(timestamp);
                    return next != null;
                }
                String n = (String) lowerKey(next, timestamp);
                if (n == null)
                    return false;
                next = n;
                return true;
            }

            @Override
            public ListAccessor peek() {
                return listAccessor(next, timestamp);
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public ListAccessor next() {
                if (next == null)
                    throw new NoSuchElementException();
                ListAccessor la = peek();
                next = (String) higherKey(next, timestamp);
                return la;
            }
        };
    }

    /**
     * Returns an iterator over the list accessors
     * with keys whose toString start with the given prefix.
     *
     * @param prefix    The qualifying prefix.
     * @param timestamp The time of the query.
     * @return The iterator.
     */
    default PeekABoo<ListAccessor> iterator(String prefix, long timestamp) {
        return iterable(prefix, timestamp).iterator();
    }

    /**
     * Returns an iterable over the list accessors
     * with keys whose toString start with the given prefix.
     *
     * @param prefix    The qualifying prefix.
     * @param timestamp The time of the query.
     * @return The iterator.
     */
    default PeekABoo<ListAccessor> iterable(String prefix, long timestamp) {
        return new PeekABoo<ListAccessor>() {

            String next = ceiling(prefix);

            private String ceiling(String state) {
                String k = (String) ceilingKey(state, timestamp);
                if (k != null && k.startsWith(prefix))
                    return k;
                return null;
            }

            @Override
            public String getPosition() {
                return next;
            }

            @Override
            public void setPosition(String state) {
                next = ceiling(state);
            }

            @Override
            public boolean positionPrior() {
                if (next == null) {
                    String n = (String) lowerKey(prefix + Character.MAX_VALUE, timestamp);
                    if (n == null || !n.startsWith(prefix))
                        return false;
                    next = n;
                    return true;
                }
                String n = (String) lowerKey(next, timestamp);
                if (n == null || !n.startsWith(prefix))
                    return false;
                next = n;
                return true;
            }

            @Override
            public ListAccessor peek() {
                return listAccessor(next);
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public ListAccessor next() {
                if (next == null)
                    throw new NoSuchElementException();
                ListAccessor la = peek();
                next = (String) higherKey(next, timestamp);
                if (next != null && !next.startsWith(prefix))
                    next = null;
                return la;
            }
        };
    }

    /**
     * Returns a map accessor for the time of the current transaction.
     *
     * @return A map accessor for the latest time.
     */
    default MapAccessor mapAccessor() {
        return mapAccessor(getTimestamp());
    }

    /**
     * Returns a map accessor for a given time.
     *
     * @param timestamp The time of the query.
     * @return A map accessor for the given time.
     */
    default MapAccessor mapAccessor(long timestamp) {
        return new MapAccessor() {

            @Override
            public long getTimestamp() {
                return timestamp;
            }

            @Override
            public Object get(Comparable key) {
                return VersionedMapNode.this.get(key, timestamp);
            }

            @Override
            public int size() {
                return VersionedMapNode.this.size(timestamp);
            }

            @Override
            public ListAccessor listAccessor(Comparable key) {
                return VersionedMapNode.this.listAccessor(key, timestamp);
            }

            @Override
            public NavigableSet<Comparable> flatKeys() {
                return VersionedMapNode.this.flatKeys(timestamp);
            }

            @Override
            public Comparable firstKey() {
                return VersionedMapNode.this.firstKey(timestamp);
            }

            @Override
            public Comparable lastKey() {
                return VersionedMapNode.this.lastKey(timestamp);
            }

            @Override
            public Comparable higherKey(Comparable key) {
                return VersionedMapNode.this.higherKey(key, timestamp);
            }

            @Override
            public Comparable ceilingKey(Comparable key) {
                return VersionedMapNode.this.ceilingKey(key, timestamp);
            }

            @Override
            public Comparable lowerKey(Comparable key) {
                return VersionedMapNode.this.lowerKey(key, timestamp);
            }

            @Override
            public Comparable floorKey(Comparable key) {
                return VersionedMapNode.this.floorKey(key, timestamp);
            }

            @Override
            public PeekABoo<ListAccessor> iterator() {
                return VersionedMapNode.this.iterator(timestamp);
            }

            @Override
            public PeekABoo<ListAccessor> iterator(final String prefix) {
                return VersionedMapNode.this.iterator(prefix, timestamp);
            }

            @Override
            public PeekABoo<ListAccessor> iterable(final String prefix) {
                return VersionedMapNode.this.iterable(prefix, timestamp);
            }

            @Override
            public NavigableMap<Comparable, List> flatMap() {
                return VersionedMapNode.this.flatMap(timestamp);
            }
        };
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
