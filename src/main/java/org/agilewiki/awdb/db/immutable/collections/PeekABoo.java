package org.agilewiki.awdb.db.immutable.collections;

import java.util.Iterator;

/**
 * An iterator that can be positioned.
 */
public interface PeekABoo<T1> extends Iterator<T1>, PeekABooable<T1> {
    String getPosition();

    void setPosition(String position);

    default boolean positionNext() {
        if (!hasNext())
            return false;
        next();
        return true;
    }

    boolean positionPrior();

    /**
     * Returns the next value that will be returned.
     *
     * @return The next value to be returned, or null.
     */
    T1 peek();

    @Override
    default PeekABoo<T1> iterator() {
        return this;
    }
}
