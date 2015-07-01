package org.agilewiki.awdb.db.immutable.scalars;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;

import java.nio.ByteBuffer;

/**
 * Defines how true is serialized / deserialized.
 */
public class BooleanFactory extends BaseFactory {
    public final char trueId;
    public final char falseId;

    public BooleanFactory(FactoryRegistry factoryRegistry, char id, char trueId, char falseId) {
        super(factoryRegistry, id);
        this.trueId = trueId;
        this.falseId = falseId;
        new TrueFactory(factoryRegistry, trueId);
        new FalseFactory(factoryRegistry, falseId);
    }

    @Override
    public ImmutableFactory getImmutableFactory(Object immutable) {
        return factoryRegistry.getImmutableFactory(
                (Boolean) immutable ? trueId : falseId);
    }

    @Override
    public Class getImmutableClass() {
        return Boolean.class;
    }

    @Override
    public int getDurableLength(Object durable) {
        return 2;
    }

    @Override
    public void serialize(Object durable, ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean deserialize(ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException();
    }
}
