package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how a nil map node is serialized / deserialized.
 */
public class VersionedNilMapNodeFactory extends BaseFactory {

    public VersionedNilMapNodeFactory(DbFactoryRegistry registry) {
        super(registry, registry.versionedNilMapId);
    }

    @Override
    public Class getImmutableClass() {
        return getClass();
    }

    @Override
    public void match(Object durable) {
        if (!((VersionedMapNode) durable).isNil())
            throw new IllegalArgumentException("The immutable object is not a nil map node");
    }

    @Override
    public int getDurableLength(Object durable) {
        return 2;
    }

    @Override
    public void serialize(Object durable, ByteBuffer byteBuffer) {
    }

    @Override
    public VersionedMapNode deserialize(ByteBuffer byteBuffer) {
        return ((DbFactoryRegistry) factoryRegistry).versionedNilMap;
    }
}
