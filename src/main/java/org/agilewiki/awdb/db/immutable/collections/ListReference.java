package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.scalars.CS256;
import org.agilewiki.awdb.db.virtualcow.BlockReference;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;

/**
 * A reference to an immutable list.
 */
public class ListReference extends BlockReference implements ListNode {
    public ListReference(DbFactoryRegistry registry,
                         int blockNbr,
                         int blockLength,
                         CS256 cs256) {
        super(registry, blockNbr, blockLength, cs256);
    }

    @Override
    public ListNodeData getData() {
        return (ListNodeData) super.getData();
    }

    protected Object loadData(ByteBuffer byteBuffer) {
        return new ListNodeData(this, byteBuffer);
    }

    /**
     * Write the durable to a byte buffer.
     *
     * @param byteBuffer The byte buffer.
     */
    public void writeDurable(ByteBuffer byteBuffer) {
        int expected = byteBuffer.position() + getDurableLength();
        byteBuffer.putChar(getRegistry().listReferenceId);
        serialize(byteBuffer);
        if (expected != byteBuffer.position()) {
            getRegistry().db.close();
            throw new SerializationException();
        }
    }
}
