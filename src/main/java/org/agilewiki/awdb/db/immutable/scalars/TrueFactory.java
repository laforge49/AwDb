package org.agilewiki.awdb.db.immutable.scalars;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how true is serialized / deserialized.
 */
public class TrueFactory extends BaseFactory {

    public TrueFactory(FactoryRegistry factoryRegistry, char id) {
        super(factoryRegistry, id);
    }

    @Override
    public Class getImmutableClass() {
        return getClass();
    }

    @Override
    public void match(Object durable) {
        if (!durable.equals(true))
            throw new IllegalArgumentException("The immutable object is not true");
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
        return true;
    }
}
