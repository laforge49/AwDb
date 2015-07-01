package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.immutable.scalars.CS256;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An immutable map of versioned lists.
 */
public class VersionedMapNodeImpl implements VersionedMapNode {

    public final DbFactoryRegistry registry;

    protected final AtomicReference<VersionedMapNodeData> dataReference = new AtomicReference<>();
    protected final int durableLength;
    protected ByteBuffer byteBuffer;

    protected VersionedMapNodeImpl(DbFactoryRegistry registry) {
        this.registry = registry;
        dataReference.set(new VersionedMapNodeData(this));
        durableLength = 2;
    }

    protected VersionedMapNodeImpl(DbFactoryRegistry registry, ByteBuffer byteBuffer) {
        this.registry = registry;
        durableLength = byteBuffer.getInt();
        this.byteBuffer = byteBuffer.slice();
        this.byteBuffer.limit(durableLength - 6);
        byteBuffer.position(byteBuffer.position() + durableLength - 6);
    }

    protected VersionedMapNodeImpl(DbFactoryRegistry registry,
                                   int level,
                                   VersionedMapNode leftNode,
                                   VersionedListNode listNode,
                                   VersionedMapNode rightNode,
                                   Comparable key) {
        this.registry = registry;
        VersionedMapNodeData data = new VersionedMapNodeData(
                this,
                level,
                leftNode,
                listNode,
                rightNode,
                key);
        durableLength = data.getDurableLength();
        dataReference.set(data);
    }

    @Override
    public DbFactoryRegistry getRegistry() {
        return registry;
    }

    @Override
    public VersionedMapNodeData getData() {
        VersionedMapNodeData data = dataReference.get();
        if (data != null)
            return data;
        dataReference.compareAndSet(null, new VersionedMapNodeData(this, byteBuffer.slice()));
        return dataReference.get();
    }

    @Override
    public int getDurableLength() {
        return durableLength;
    }

    @Override
    public void writeDurable(ByteBuffer byteBuffer) {
        if (isNil()) {
            byteBuffer.putChar(getRegistry().versionedNilMapId);
            return;
        }
        byteBuffer.putChar(getRegistry().versionedMapNodeImplId);
        serialize(byteBuffer);
    }

    @Override
    public void serialize(ByteBuffer byteBuffer) {
        byteBuffer.putInt(getDurableLength());
        if (this.byteBuffer == null) {
            getData().serialize(byteBuffer);
            return;
        }
        ByteBuffer bb = byteBuffer.slice();
        bb.limit(durableLength - 6);
        byteBuffer.put(this.byteBuffer.slice());
        this.byteBuffer = bb;
        dataReference.set(null); //limit memory footprint, plugs memory leak.
    }

    @Override
    public Object shrink() {
        Db db = registry.db;
        VersionedMapNodeData data = getData();
        ByteBuffer byteBuffer = ByteBuffer.allocate(durableLength - 6);
        data.serialize(byteBuffer);
        byteBuffer.flip();
        CS256 cs256 = new CS256(byteBuffer);
        int blockNbr = db.allocate();
        db.writeBlock(byteBuffer, blockNbr);
        return new VersionedMapReference(registry, blockNbr, durableLength - 6, cs256);
    }
}
