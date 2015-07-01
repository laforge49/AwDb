package org.agilewiki.awdb.db.immutable;

import java.nio.ByteBuffer;

/**
 * Defines how a class of immutable is serialized / deserialized.
 */
public interface ImmutableFactory {

    /**
     * Returns a char used to identify the durable factory in a serialized object.
     *
     * @return A char used to identify the factory.
     */
    char getId();

    /**
     * Returns the class of an immutable that the factory serializes / deserializes.
     *
     * @return The immutable class.
     */
    Class getImmutableClass();

    /**
     * Returns a value-specific factory.
     *
     * @param immutable The immutable object.
     * @return The durable factory specific to the value.
     */
    default ImmutableFactory getImmutableFactory(Object immutable) {
        return this;
    }

    /**
     * Returns a value-specific durable id.
     *
     * @param durable The immutable object.
     * @return The value-specific durable id.
     */
    default char getId(Object durable) {
        return getId();
    }

    /**
     * Validate that the immutable object is a match for the factory.
     *
     * @param immutable The immutable object.
     * @throws IllegalArgumentException when the object is not a match.
     */
    default void match(Object immutable) {
        if (immutable.getClass() != getImmutableClass())
            throw new IllegalArgumentException("The immutable object is not a match");
    }

    /**
     * Returns the size of a byte array needed to serialize the durable object,
     * including the space needed for the durable id.
     *
     * @param immutable The immutable object to be serialized.
     * @return The size in bytes of the serialized data.
     */
    int getDurableLength(Object immutable);

    /**
     * Write the immutable to a byte buffer.
     *
     * @param immutable  The immutable object to be serialized.
     * @param byteBuffer The byte buffer.
     */
    default void writeDurable(Object immutable, ByteBuffer byteBuffer) {
        if (immutable == null) {
            byteBuffer.putChar(FactoryRegistry.NULL_ID);
            return;
        }
        byteBuffer.putChar(getId(immutable));
        serialize(immutable, byteBuffer);
    }

    /**
     * Serialize an immutable object into a ByteBuffer.
     *
     * @param immutable  The immutable object to be serialized.
     * @param byteBuffer Where the serialized data is to be placed.
     */
    void serialize(Object immutable, ByteBuffer byteBuffer);

    /**
     * Deserialize an immutable object from the content of a ByteBuffer.
     *
     * @param byteBuffer Holds the data used to create the immutable object.
     * @return The deserialized object.
     */
    Object deserialize(ByteBuffer byteBuffer);

    /**
     * Returns a ByteBuffer loaded with the serialized contents of the immutable.
     *
     * @param immutable    The immutable to be serialized.
     * @return The loaded ByteBuffer.
     */
    default ByteBuffer toByteBuffer(Object immutable) {
        ByteBuffer bb = ByteBuffer.allocate(getDurableLength(immutable));
        writeDurable(immutable, bb);
        bb.flip();
        return bb;
    }
}
