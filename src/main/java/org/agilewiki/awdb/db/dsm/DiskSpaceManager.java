package org.agilewiki.awdb.db.dsm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashSet;

/**
 * A durable class for managing up to a million blocks.
 * And if each block is 1 MB in length, DSM can then manage
 * a 1TB file.
 */
public class DiskSpaceManager {
    final BitSet bitSet;

    HashSet<Integer> freed = new HashSet<Integer>();

    /**
     * Create a DSM with no space allocated.
     */
    public DiskSpaceManager() {
        bitSet = new BitSet();
    }

    /**
     * Read a bit array from a ByteBuffer.
     * The bit array indicates which blocks are in use.
     * The ByteBuffer contains first a byte count pf the size of the bit array,
     * followed by the bytes of the bit array.
     *
     * @param byteBuffer The source of the bit array.
     */
    public DiskSpaceManager(ByteBuffer byteBuffer) {
        int byteCount = byteBuffer.getInt();
        ByteBuffer bb = byteBuffer.slice();
        bb.limit(byteCount);
        bitSet = BitSet.valueOf(bb);
        byteBuffer.position(byteBuffer.position() + byteCount);
    }

    /**
     * Allocates a block of disk space.
     *
     * @return The number of the block that was allocated.
     */
    public int allocate() {
        int i = bitSet.nextClearBit(0);
        if (i > 999999) {
            Logger logger = LoggerFactory.getLogger(getClass());
            logger.error("Out of space");
            throw new OutOfSpaceException();
        }
        bitSet.set(i);
        return i;
    }

    /**
     * Returns the number of allocated pages.
     *
     * @return The number of pages in use.
     */
    public int usage() {
        return bitSet.cardinality();
    }

    /**
     * Release a block.
     * It will become available on the next transaction.
     *
     * @param i The block to be released.
     */
    public void release(int i) {
        if (!bitSet.get(i)) {
            Logger logger = LoggerFactory.getLogger(getClass());
            logger.error("attempt to release an unallocated block");
            throw new ReleasingUnallocatedBlockException();
        }
        if (freed.contains(i)) {
            Logger logger = LoggerFactory.getLogger(getClass());
            logger.error("attempt to release a block a second time");
            throw new DuplicateReleaseException();
        }
        freed.add(i);
    }

    /**
     * Returns the number of bytes needed to save the allocation data,
     * plus 4 for the length.
     *
     * @return The number of bytes needed to save the bit array.
     */
    public int durableLength() {
        return (bitSet.length() + 7) / 8 + 4;
    }

    /**
     * Make the blocks available which had been released.
     * <p>
     * Calling commit will sometimes reduce the durable length.
     * So commit should be called before durableLength if an accurate
     * result is needed for a subsequent write.
     * </p>
     */
    public void commit() {
        for (int i : freed) {
            bitSet.clear(i);
        }
        freed.clear();
    }

    /**
     * Write the bit array to a ByteBuffer.
     *
     * @param byteBuffer Holds the bit array.
     */
    public void write(ByteBuffer byteBuffer) {
        byte[] bytes = bitSet.toByteArray();
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
    }
}
