package org.agilewiki.awdb.db.immutable.scalars;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how an Float is serialized / deserialized.
 */
public class FloatFactory extends BaseFactory {

    /**
     * Create and register the factory.
     *
     * @param factoryRegistry The registry where the factory is registered.
     * @param id              The char used to identify the factory.
     */
    public FloatFactory(FactoryRegistry factoryRegistry, char id) {
        super(factoryRegistry, id);
    }

    @Override
    public Class getImmutableClass() {
        return Float.class;
    }

    @Override
    public int getDurableLength(Object durable) {
        if (durable == null)
            return 2;
        return 6;
    }

    @Override
    public void serialize(Object durable, ByteBuffer byteBuffer) {
        byteBuffer.putFloat((Float) durable);
    }

    @Override
    public Float deserialize(ByteBuffer byteBuffer) {
        return byteBuffer.getFloat();
    }
}
