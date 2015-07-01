package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.immutable.collections.Releasable;
import org.agilewiki.awdb.db.immutable.scalars.CS256;
import org.agilewiki.awdb.db.immutable.scalars.CS256Factory;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;

/**
 * Holds a block number, block length and checksum.
 */
public class BlockReference implements Releasable {

    public final DbFactoryRegistry registry;

    /**
     * The number of the block being referenced.
     */
    final public int blockNbr;

    /**
     * The length of the durable data held by the block.
     */
    final public int blockLength;

    /**
     * The checksum of the contents of the block.
     */
    final public CS256 cs256;

    protected SoftReference softReference;

    protected final CS256Factory cs256Factory;

    /**
     * Create a reference to an existing block.
     *
     * @param registry    The registry for the database.
     * @param blockNbr    The number of the block being referenced.
     * @param blockLength The length of the durable data held by the block.
     * @param cs256       The checksum of the contents of the block.
     */
    public BlockReference(DbFactoryRegistry registry,
                          int blockNbr,
                          int blockLength,
                          CS256 cs256) {
        this.registry = registry;
        this.blockNbr = blockNbr;
        this.blockLength = blockLength;
        this.cs256 = cs256;
        cs256Factory = (CS256Factory) registry.getImmutableFactory(cs256);
    }

    /**
     * Creates a new block and a reference to it.
     *
     * @param registry  The registry for the database.
     * @param immutable The object to be saved in the new block.
     */
    public BlockReference(DbFactoryRegistry registry, Object immutable) {
        ImmutableFactory factory = registry.getImmutableFactory(immutable);
        this.registry = registry;
        Db db = registry.db;
        int bl = factory.getDurableLength(immutable);
        if (bl > db.maxBlockSize && immutable instanceof Releasable) {
            immutable = ((Releasable) immutable).resize(db.maxBlockSize, db.maxBlockSize);
            bl = factory.getDurableLength(immutable);
        }
        if (bl > db.maxBlockSize) {
            db.close();
            db.getReactor().error("block size exceeds max block size");
            throw new BlockSizeTooLarge();
        }
        blockLength = bl;
        ByteBuffer byteBuffer = ByteBuffer.allocate(blockLength);
        factory.writeDurable(immutable, byteBuffer);
        byteBuffer.flip();
        cs256 = new CS256(byteBuffer);
        cs256Factory = (CS256Factory) registry.getImmutableFactory(cs256);
        blockNbr = db.allocate();
        db.writeBlock(byteBuffer, blockNbr);
    }

    public DbFactoryRegistry getRegistry() {
        return registry;
    }

    /**
     * Releases the contents of the block as well as the block.
     */
    @Override
    public void releaseAll() {
        Object immutable = getData();
        if (immutable instanceof Releasable)
            ((Releasable) immutable).releaseAll();
        releaseLocal();
    }

    /**
     * Releases the block.
     */
    @Override
    public void releaseLocal() {
        registry.db.release(blockNbr);
    }

    /**
     * Reads, validates, deserializes and returns the contents of the block.
     *
     * @return The contents of the block.
     */
    public Object getData() {
        if (softReference != null) {
            Object immutable = softReference.get();
            if (immutable != null)
                return immutable;
        }
        Db db = registry.db;
        ByteBuffer byteBuffer = ByteBuffer.allocate(blockLength);
        db.readBlock(byteBuffer, blockNbr);
        byteBuffer.flip();
        CS256 cs = new CS256(byteBuffer);
        if (!cs256.equals(cs)) {
            if (db.isPrivileged())
                db.close();
            db.getReactor().error("block has bad checksum");
            throw new UnexpectedChecksumException();
        }
        Object immutable = loadData(byteBuffer);
        softReference = new SoftReference(immutable);
        return immutable;
    }

    protected Object loadData(ByteBuffer byteBuffer) {
        ImmutableFactory factory = registry.readId(byteBuffer);
        return factory.deserialize(byteBuffer);
    }

    /**
     * Serialize this object into a ByteBuffer.
     *
     * @param byteBuffer Where the serialized data is to be placed.
     */
    public void serialize(ByteBuffer byteBuffer) {
        byteBuffer.putInt(blockNbr);
        byteBuffer.putInt(blockLength);
        cs256Factory.writeDurable(cs256, byteBuffer);
    }

    /**
     * Returns the size of a byte array needed to serialize this object,
     * including the space needed for the durable id.
     *
     * @return The size in bytes of the serialized data.
     */
    public int getDurableLength() {
        return 2 + 4 + 4 + CS256Factory.DURABLE_LENGTH;
    }
}
