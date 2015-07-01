package org.agilewiki.awdb.db.immutable.scalars;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how a Double is serialized / deserialized.
 */
public class DoubleFactory extends BaseFactory {

    public DoubleFactory(FactoryRegistry factoryRegistry, char id) {
        super(factoryRegistry, id);
    }

    @Override
    public Class getImmutableClass() {
        return Double.class;
    }

    @Override
    public int getDurableLength(Object durable) {
        if (durable == null)
            return 2;
        return 10;
    }

    @Override
    public void serialize(Object durable, ByteBuffer byteBuffer) {
        byteBuffer.putDouble((Double) durable);
    }

    @Override
    public Double deserialize(ByteBuffer byteBuffer) {
        return byteBuffer.getDouble();
    }
}
