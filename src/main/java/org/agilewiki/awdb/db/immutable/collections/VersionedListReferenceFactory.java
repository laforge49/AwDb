package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.scalars.CS256;
import org.agilewiki.awdb.db.virtualcow.BlockReference;
import org.agilewiki.awdb.db.virtualcow.BlockReferenceFactory;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

/**
 * Defines how a versioned list reference is serialized / deserialized.
 */
public class VersionedListReferenceFactory extends BlockReferenceFactory {

    /**
     * Create and register the factory.
     *
     * @param registry The registry where the factory is registered.
     */
    public VersionedListReferenceFactory(DbFactoryRegistry registry) {
        super(registry, registry.versionedListReferenceId);
    }

    @Override
    public Class getImmutableClass() {
        return VersionedListReference.class;
    }

    protected BlockReference createReference(DbFactoryRegistry registry,
                                             int blockNbr,
                                             int blockLength,
                                             CS256 cs256) {
        return new VersionedListReference(registry, blockNbr, blockLength, cs256);
    }
}
