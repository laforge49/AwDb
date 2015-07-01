package org.agilewiki.awdb.db.immutable.scalars;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * An immutable 256-bit checksum inspired by zfs.
 */
public class CS256 {
    private final BitSet bitSet;

    /**
     * Create a checksum of the contents of the bytebuffer.
     *
     * @param byteBuffer ByteBuffer is read but its position is not altered.
     */
    public CS256(ByteBuffer byteBuffer) {
        bitSet = new BitSet(256);
        bitSet.flip(255);
        int length = byteBuffer.remaining();
        int offset = byteBuffer.position();
        for (int i = 0; i < length; i++) {
            bitSet.flip((((int) byteBuffer.get(offset + i)) - (int) Byte.MIN_VALUE + i * 7) % 256);
        }
        long[] a = toLongArray();
    }

    /**
     * Load the contents of the checksum with an array of 4 longs.
     *
     * @param longs
     */
    public CS256(long[] longs) {
        if (longs.length != 4)
            throw new IllegalArgumentException("length of longs must be 4");
        bitSet = BitSet.valueOf(longs);
    }

    @Override
    public boolean equals(Object obj) {
        return bitSet.equals(((CS256) obj).bitSet);
    }

    /**
     * Returns the contents of the checksum.
     *
     * @return An array, long[4].
     */
    public long[] toLongArray() {
        return bitSet.toLongArray();
    }
}
