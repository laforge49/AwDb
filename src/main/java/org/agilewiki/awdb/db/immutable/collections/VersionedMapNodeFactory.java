package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;

/**
 * Defines how true is serialized / deserialized.
 */
public class VersionedMapNodeFactory extends BaseFactory {

    public final VersionedMapNode versionedNilMap;
    public final VersionedNilMapNodeFactory versionedNilMapNodeFactory;

    public VersionedMapNodeFactory(
            DbFactoryRegistry registry) {
        super(registry, registry.versionedMapNodeImplId);
        versionedNilMapNodeFactory = new VersionedNilMapNodeFactory(registry);
        versionedNilMap = new VersionedMapNodeImpl(registry);
        new VersionedMapReferenceFactory(registry);
    }

    @Override
    public ImmutableFactory getImmutableFactory(Object immutable) {
        if (((VersionedMapNode) immutable).isNil())
            return versionedNilMapNodeFactory;
        return this;
    }

    @Override
    public Class getImmutableClass() {
        return VersionedMapNodeImpl.class;
    }

    @Override
    public int getDurableLength(Object durable) {
        return ((VersionedMapNode) durable).getDurableLength();
    }

    @Override
    public void serialize(Object durable, ByteBuffer byteBuffer) {
        ((VersionedMapNode) durable).serialize(byteBuffer);
    }

    @Override
    public VersionedMapNode deserialize(ByteBuffer byteBuffer) {
        return new VersionedMapNodeImpl((DbFactoryRegistry) factoryRegistry, byteBuffer);
    }
}
