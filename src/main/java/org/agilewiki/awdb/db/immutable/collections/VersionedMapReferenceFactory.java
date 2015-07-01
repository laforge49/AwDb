package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.scalars.CS256;
import org.agilewiki.awdb.db.virtualcow.BlockReference;
import org.agilewiki.awdb.db.virtualcow.BlockReferenceFactory;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

/**
 * Defines how a versioned map reference is serialized / deserialized.
 */
public class VersionedMapReferenceFactory extends BlockReferenceFactory {

    /**
     * Create and register the factory.
     *
     * @param registry The registry where the factory is registered.
     */
    public VersionedMapReferenceFactory(DbFactoryRegistry registry) {
        super(registry, registry.versionedMapReferenceId);
    }

    @Override
    public Class getImmutableClass() {
        return VersionedMapReference.class;
    }

    protected BlockReference createReference(DbFactoryRegistry registry,
                                             int blockNbr,
                                             int blockLength,
                                             CS256 cs256) {
        return new VersionedMapReference(registry, blockNbr, blockLength, cs256);
    }
}
