package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how a list is serialized / deserialized.
 */
public class ListNodeFactory extends BaseFactory {

    public final ListNode nilList;
    public final NilListNodeFactory nilListNodeFactory;

    public ListNodeFactory(DbFactoryRegistry registry) {
        super(registry, registry.listNodeImplId);
        nilListNodeFactory = new NilListNodeFactory(registry);
        nilList = new ListNodeImpl(registry);
        new ListReferenceFactory(registry);
    }

    @Override
    public ImmutableFactory getImmutableFactory(Object immutable) {
        if (((ListNode) immutable).isNil())
            return nilListNodeFactory;
        return this;
    }

    @Override
    public Class getImmutableClass() {
        return ListNodeImpl.class;
    }

    @Override
    public int getDurableLength(Object immutable) {
        return ((ListNode) immutable).getDurableLength();
    }

    @Override
    public void serialize(Object immutable, ByteBuffer byteBuffer) {
        ((ListNode) immutable).serialize(byteBuffer);
    }

    @Override
    public ListNode deserialize(ByteBuffer byteBuffer) {
        return new ListNodeImpl((DbFactoryRegistry) factoryRegistry, byteBuffer);
    }
}
