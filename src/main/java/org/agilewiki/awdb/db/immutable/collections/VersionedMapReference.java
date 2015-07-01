package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.scalars.CS256;
import org.agilewiki.awdb.db.virtualcow.BlockReference;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;

/**
 * A reference to an immutable map of versioned lists.
 */
public class VersionedMapReference extends BlockReference implements VersionedMapNode {
    public VersionedMapReference(DbFactoryRegistry registry,
                                 int blockNbr,
                                 int blockLength,
                                 CS256 cs256) {
        super(registry, blockNbr, blockLength, cs256);
    }

    @Override
    public VersionedMapNodeData getData() {
        return (VersionedMapNodeData) super.getData();
    }

    protected Object loadData(ByteBuffer byteBuffer) {
        return new VersionedMapNodeData(this, byteBuffer);
    }

    /**
     * Write the durable to a byte buffer.
     *
     * @param byteBuffer The byte buffer.
     */
    public void writeDurable(ByteBuffer byteBuffer) {
        int expected = byteBuffer.position() + getDurableLength();
        byteBuffer.putChar(getRegistry().versionedMapReferenceId);
        serialize(byteBuffer);
        if (expected != byteBuffer.position()) {
            getRegistry().db.close();
            throw new SerializationException();
        }
    }
}
