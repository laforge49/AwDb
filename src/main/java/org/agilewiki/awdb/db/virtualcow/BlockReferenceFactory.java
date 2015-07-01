package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.immutable.scalars.CS256;
import org.agilewiki.awdb.db.immutable.scalars.CS256Factory;

import java.nio.ByteBuffer;

/**
 * Defines how a BlockReference is serialized / deserialized.
 */
public class BlockReferenceFactory extends BaseFactory {

    /**
     * the CS256 factory.
     */
    final public CS256Factory cs256Factory;

    /**
     * Create and register the factory.
     *
     * @param registry        The registry where the factory is registered.
     */
    public BlockReferenceFactory(DbFactoryRegistry registry) {
        this(registry, registry.blockReferenceFactoryId);
    }

    /**
     * Create and register the factory.
     *
     * @param registry        The registry where the factory is registered.
     * @param id              The char used to identify the factory.
     */
    protected BlockReferenceFactory(DbFactoryRegistry registry, char id) {
        super(registry, id);
        cs256Factory = (CS256Factory) registry.getImmutableFactory(CS256.class);
    }

    @Override
    public char getId() {
        return id;
    }

    @Override
    public Class getImmutableClass() {
        return BlockReference.class;
    }

    @Override
    public int getDurableLength(Object immutable) {
        return ((BlockReference) immutable).getDurableLength();
    }

    @Override
    public void serialize(Object immutable, ByteBuffer byteBuffer) {
        ((BlockReference) immutable).serialize(byteBuffer);
    }

    @Override
    public Object deserialize(ByteBuffer byteBuffer) {
        int blockNbr = byteBuffer.getInt();
        int blockLength = byteBuffer.getInt();
        ImmutableFactory factory = factoryRegistry.readId(byteBuffer);
        CS256 cs256 = (CS256) factory.deserialize(byteBuffer);
        return createReference((DbFactoryRegistry) factoryRegistry, blockNbr, blockLength, cs256);
    }

    protected BlockReference createReference(DbFactoryRegistry registry,
                                             int blockNbr,
                                             int blockLength,
                                             CS256 cs256) {
        return new BlockReference(registry, blockNbr, blockLength, cs256);
    }
}
