package org.agilewiki.awdb.db.immutable;

import java.nio.ByteBuffer;

/**
 * A durable factory registry.
 */
public interface FactoryRegistry {

    /**
     * A time after all insertions and deletions.
     */
    public final static long MAX_TIMESTAMP = Long.MAX_VALUE - 1;

    public static final char NULL_ID = 'N';

    /**
     * Register a durable factory.
     *
     * @param factory The factory to be registered.
     */
    void register(ImmutableFactory factory);

    /**
     * Returns the factory for the given id.
     *
     * @param id The id of an immutable factory.
     * @return The immutable factory, or null.
     */
    ImmutableFactory getImmutableFactory(char id);

    /**
     * Map an immutable object to an immutable factory instance.
     * Nulls are mapped to the registered NullFactory.
     *
     * @param immutable The immutable object, or null.
     * @return The immutable factory.
     * @throws IllegalArgumentException when the immutable class is not recognized.
     */
    ImmutableFactory getImmutableFactory(Object immutable);

    /**
     * Map an immutable class to an immutable factory instance.
     *
     * @param immutableClass The immutable class.
     * @return The immutable factory.
     * @throws IllegalArgumentException when the immutable class is not recognized.
     */
    ImmutableFactory getImmutableFactory(Class immutableClass);

    /**
     * Read an id and map it to a durable factory instance.
     *
     * @param byteBuffer The byte buffer to be read.
     * @return The durable factory instance.
     * @throws IllegalStateException when the durable id is not recognized.
     */
    ImmutableFactory readId(ByteBuffer byteBuffer);
}
