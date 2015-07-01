package org.agilewiki.awdb.db.immutable.scalars;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how null is serialized / deserialized.
 */
public class NullFactory extends BaseFactory {

    /**
     * Create and register this factory.
     *
     * @param factoryRegistry The registry where this factory is registered.
     * @param id              The char used to identify this factory.
     */
    public NullFactory(FactoryRegistry factoryRegistry, char id) {
        super(factoryRegistry, id);
    }

    @Override
    public Class getImmutableClass() {
        return getClass();
    }

    @Override
    public void match(Object durable) {
        if (durable != null)
            throw new IllegalArgumentException("The immutable object is not null");
    }

    @Override
    public int getDurableLength(Object durable) {
        return 2;
    }

    @Override
    public void serialize(Object durable, ByteBuffer byteBuffer) {
    }

    @Override
    public Void deserialize(ByteBuffer byteBuffer) {
        return null;
    }
}
