package org.agilewiki.awdb.db.immutable.scalars;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how an Integer is serialized / deserialized.
 */
public class IntegerFactory extends BaseFactory {

    public IntegerFactory(FactoryRegistry factoryRegistry, char id) {
        super(factoryRegistry, id);
    }

    @Override
    public Class getImmutableClass() {
        return Integer.class;
    }

    @Override
    public int getDurableLength(Object durable) {
        if (durable == null)
            return 2;
        return 6;
    }

    @Override
    public void serialize(Object durable, ByteBuffer byteBuffer) {
        byteBuffer.putInt((Integer) durable);
    }

    @Override
    public Integer deserialize(ByteBuffer byteBuffer) {
        return byteBuffer.getInt();
    }
}
