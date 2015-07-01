package org.agilewiki.awdb.db.immutable.collections;

import java.util.List;

/**
 * Accesses a list.
 */
public interface ListAccessor extends PeekABooable {

    /**
     * The key for the list.
     *
     * @return The key for the list, or null.
     */
    Comparable key();

    /**
     * Returns the time being accessed.
     *
     * @return The time being accessed, or MAX_TIMESTAMP.
     */
    long getTimestamp();

    /**
     * Returns the count of all the values currently in the list.
     *
     * @return The current size of the list.
     */
    int size();

    /**
     * Returns a value if it is in range and the value exists for the given time.
     *
     * @param ndx The index of the selected value.
     * @return A value, or null.
     */
    Object get(int ndx);

    /**
     * Get the index of an existing value with the same identity (==).
     * (The list is searched in order.)
     *
     * @param value The value sought.
     * @return The index, or -1.
     */
    int getIndex(Object value);

    /**
     * Get the index of an existing value with the same identity (==).
     * (The list is searched in reverse order.)
     *
     * @param value The value sought.
     * @return The index, or -1.
     */
    int getIndexRight(Object value);

    /**
     * Find the index of an equal existing value.
     * (The list is searched in order.)
     *
     * @param value The value sought.
     * @return The index, or -1.
     */
    int findIndex(Object value);

    /**
     * Find the index of an equal existing value.
     * (The list is searched in reverse order.)
     *
     * @param value The value sought.
     * @return The index, or -1.
     */
    int findIndexRight(Object value);

    /**
     * Returns the index of an existing value higher than the given index.
     *
     * @param ndx A given index.
     * @return An index of an existing value that is higher, or -1.
     */
    int higherIndex(int ndx);

    /**
     * Returns the index of an existing value higher than or equal to the given index.
     *
     * @param ndx A given index.
     * @return An index of an existing value that is higher or equal, or -1.
     */
    int ceilingIndex(int ndx);

    /**
     * Returns the index of the first existing value in the list.
     *
     * @return The index of the first existing value in the list, or -1.
     */
    int firstIndex();

    /**
     * Returns the index of an existing value lower than the given index.
     *
     * @param ndx A given index.
     * @return An index of an existing value that is lower, or -1.
     */
    int lowerIndex(int ndx);

    /**
     * Returns the index of an existing value lower than or equal to the given index.
     *
     * @param ndx A given index.
     * @return An index of an existing value that is lower or equal, or -1.
     */
    int floorIndex(int ndx);

    /**
     * Returns the index of the last existing value in the list.
     *
     * @return The index of the last existing value in the list, or -1.
     */
    int lastIndex();

    /**
     * Returns true if there are no values present for the given time.
     *
     * @return Returns true if the list is empty for the given time.
     */
    boolean isEmpty();

    /**
     * Returns a list of all the values that are present for a given time.
     *
     * @return A list of all values present for the given time.
     */
    List flatList();

    /**
     * Returns an iterator over the existing values.
     *
     * @return The iterator.
     */
    PeekABoo iterator();
}
