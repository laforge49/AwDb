package org.agilewiki.awdb.db.immutable.collections;

import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Accesses a map list.
 */
public interface MapAccessor extends PeekABooable<ListAccessor> {

    /**
     * Returns the time being accessed.
     *
     * @return The time being accessed, or MAX_TIMESTAMP.
     */
    long getTimestamp();

    /**
     * Returns the selected object.
     *
     * @param key The key of the selected object.
     * @return The object, or null.
     */
    Object get(Comparable key);

    /**
     * Returns the count of all the keys with a non-empty list.
     *
     * @return The current size of the map.
     */
    int size();

    /**
     * Returns a list accessor for the given time.
     *
     * @param key The key for the list.
     * @return A list accessor for the given time, or null.
     */
    ListAccessor listAccessor(Comparable key);

    /**
     * Returns a set of all keys with non-empty lists for the given time.
     *
     * @return A set of the keys with content at the time of the query.
     */
    NavigableSet<Comparable> flatKeys();

    /**
     * Returns the smallest key of the non-empty lists for the given time.
     *
     * @return The smallest key, or null.
     */
    Comparable firstKey();

    /**
     * Returns the largest key of the non-empty lists for the given time.
     *
     * @return The largest key, or null.
     */
    Comparable lastKey();

    /**
     * Returns the next greater key, or null.
     *
     * @param key The given key.
     * @return The next greater key with content at the time of the query.
     */
    Comparable higherKey(Comparable key);

    /**
     * Returns the key with content that is greater than or equal to the given key.
     *
     * @param key The given key.
     * @return The key greater than or equal to the given key, or null.
     */
    Comparable ceilingKey(Comparable key);

    /**
     * Returns the next smaller key, or null.
     *
     * @param key The given key.
     * @return The next smaller key with content at the time of the query.
     */
    Comparable lowerKey(Comparable key);

    /**
     * Returns the key with content that is smaller than or equal to the given key.
     *
     * @param key The given key.
     * @return The key smaller than or equal to the given key, or null.
     */
    Comparable floorKey(Comparable key);

    /**
     * Returns an iterator over the non-empty list accessors.
     *
     * @return The iterator.
     */
    PeekABoo<ListAccessor> iterator();

    /**
     * Returns an iterator over the list accessors
     * with keys whose toString start with the given prefix.
     *
     * @param prefix    The qualifying prefix.
     * @return The iterator.
     */
    PeekABoo<ListAccessor> iterator(final String prefix);

    /**
     * Returns an iterable over the list accessors
     * with keys whose toString start with the given prefix.
     *
     * @param prefix    The qualifying prefix.
     * @return The iterator.
     */
    PeekABoo<ListAccessor> iterable(final String prefix);

    /**
     * Returns a map of all the keys and values present at the given time.
     *
     * @return A map of lists.
     */
    NavigableMap<Comparable, List> flatMap();
}
