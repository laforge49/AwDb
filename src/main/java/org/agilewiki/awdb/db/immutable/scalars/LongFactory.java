package org.agilewiki.awdb.db.immutable.scalars;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how a Long is serialized / deserialized.
 */
public class LongFactory extends BaseFactory {

    /**
     * Create and register the factory.
     *
     * @param factoryRegistry The registry where the factory is registered.
     * @param id              The char used to identify the factory.
     */
    public LongFactory(FactoryRegistry factoryRegistry, char id) {
        super(factoryRegistry, id);
    }

    @Override
    public Class getImmutableClass() {
        return Long.class;
    }

    @Override
    public int getDurableLength(Object durable) {
        if (durable == null)
            return 2;
        return 10;
    }

    @Override
    public void serialize(Object durable, ByteBuffer byteBuffer) {
        byteBuffer.putLong((Long) durable);
    }

    @Override
    public Long deserialize(ByteBuffer byteBuffer) {
        return byteBuffer.getLong();
    }
}
