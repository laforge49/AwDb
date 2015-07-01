package org.agilewiki.awdb.db.immutable.collections;

/**
 * A PeekABoo extension of Iterable.
 */
public interface PeekABooable<T1> extends Iterable<T1> {
    @Override
    PeekABoo<T1> iterator();
}
