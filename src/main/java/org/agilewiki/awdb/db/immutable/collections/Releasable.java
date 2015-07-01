package org.agilewiki.awdb.db.immutable.collections;

/**
 * Immutables supporting the release and resize methods.
 */
public interface Releasable {

    /**
     * Returns the size of a byte array needed to serialize this object,
     * including the space needed for the durable id.
     *
     * @return The size in bytes of the serialized data.
     */
    int getDurableLength();

    /**
     * release all resources.
     */
    void releaseAll();

    /**
     * release the local resources.
     */
    default void releaseLocal() {}

    /**
     * Resize immutables which are too large.
     *
     * @param maxSize    Max size allowed for durable length.
     * @param maxBlockSize Maximum block size.
     * @return The revised structure.
     */
    default Object resize(int maxSize, int maxBlockSize) {
        throw new UnsupportedOperationException("unable to resize");
    }

    /**
     * Creates a block reference from an immutable.
     * This method is only called on immutables that have
     * a durable length that is not larger than the max block size.
     *
     * @return A block reference.
     */
    default Object shrink() {
        throw new UnsupportedOperationException("Unable to shrink");
    }
}
