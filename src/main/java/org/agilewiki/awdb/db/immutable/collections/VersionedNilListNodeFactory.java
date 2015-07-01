package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how a nil versioned list node is serialized / deserialized.
 */
public class VersionedNilListNodeFactory extends BaseFactory {

    public VersionedNilListNodeFactory(DbFactoryRegistry registry) {
        super(registry, registry.versionedNilListId);
    }

    @Override
    public Class getImmutableClass() {
        return getClass();
    }

    @Override
    public void match(Object immutable) {
        if (!((VersionedListNode) immutable).isNil())
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
    public VersionedListNode deserialize(ByteBuffer byteBuffer) {
        return ((DbFactoryRegistry) factoryRegistry).versionedNilList;
    }
}
