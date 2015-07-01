package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how a nil list node is serialized / deserialized.
 */
public class NilListNodeFactory extends BaseFactory {

    public NilListNodeFactory(DbFactoryRegistry registry) {
        super(registry, registry.nilListId);
    }

    @Override
    public Class getImmutableClass() {
        return getClass();
    }

    @Override
    public void match(Object immutable) {
        if (!((ListNode) immutable).isNil())
            throw new IllegalArgumentException("The immutable object is not a nil list node");
    }

    @Override
    public int getDurableLength(Object immutable) {
        return 2;
    }

    @Override
    public void serialize(Object immutable, ByteBuffer byteBuffer) {
    }

    @Override
    public ListNode deserialize(ByteBuffer byteBuffer) {
        return ((DbFactoryRegistry) factoryRegistry).nilList;
    }
}
