package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how a versioned list is serialized / deserialized.
 */
public class VersionedListNodeFactory extends BaseFactory {

    public final VersionedNilListNodeFactory versionedNilListNodeFactory;
    public final VersionedListNode versionedNilList;

    public VersionedListNodeFactory(DbFactoryRegistry registry) {
        super(registry, registry.versionedListNodeImplId);
        versionedNilListNodeFactory = new VersionedNilListNodeFactory(registry);
        versionedNilList = new VersionedListNodeImpl(registry);
        new VersionedListReferenceFactory(registry);
    }

    @Override
    public ImmutableFactory getImmutableFactory(Object immutable) {
        if (((VersionedListNode) immutable).isNil())
            return versionedNilListNodeFactory;
        return this;
    }

    @Override
    public Class getImmutableClass() {
        return VersionedListNodeImpl.class;
    }

    @Override
    public int getDurableLength(Object immutable) {
        return ((VersionedListNode) immutable).getDurableLength();
    }

    @Override
    public void serialize(Object immutable, ByteBuffer byteBuffer) {
        ((VersionedListNode) immutable).serialize(byteBuffer);
    }

    @Override
    public VersionedListNode deserialize(ByteBuffer byteBuffer) {
        return new VersionedListNodeImpl((DbFactoryRegistry) factoryRegistry, byteBuffer);
    }
}
