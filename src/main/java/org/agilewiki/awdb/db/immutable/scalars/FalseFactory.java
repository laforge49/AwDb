package org.agilewiki.awdb.db.immutable.scalars;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how false is serialized / deserialized.
 */
public class FalseFactory extends BaseFactory {

    public FalseFactory(FactoryRegistry factoryRegistry, char id) {
        super(factoryRegistry, id);
    }

    @Override
    public Class getImmutableClass() {
        return getClass();
    }

    @Override
    public void match(Object durable) {
        if (!durable.equals(false))
            throw new IllegalArgumentException("The immutable object is not false");
    }

    @Override
    public int getDurableLength(Object durable) {
        return 2;
    }

    @Override
    public void serialize(Object durable, ByteBuffer byteBuffer) {
    }

    @Override
    public Boolean deserialize(ByteBuffer byteBuffer) {
        return false;
    }
}
