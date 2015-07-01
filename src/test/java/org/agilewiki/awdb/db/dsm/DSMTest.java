package org.agilewiki.awdb.db.dsm;

import junit.framework.TestCase;

import java.nio.ByteBuffer;

public class DSMTest extends TestCase {
    public void test() throws Exception {
        DiskSpaceManager dsm = new DiskSpaceManager();
        dsm.commit();
        assertEquals(4, dsm.durableLength());
        ByteBuffer bb0 = ByteBuffer.allocate(dsm.durableLength());
        dsm.write(bb0);
        assertEquals(4, bb0.position());
        bb0.flip();
        dsm = new DiskSpaceManager(bb0);
        assertEquals(4, bb0.position());
        assertEquals(0, dsm.usage());
        for (int i = 0; i < 8; i++) {
            assertEquals(i, dsm.allocate());
        }
        dsm.commit();
        assertEquals(5, dsm.durableLength());
        assertEquals(8, dsm.allocate());
        dsm.commit();
        assertEquals(6, dsm.durableLength());
        assertEquals(9, dsm.usage());
        dsm.release(8);
        assertEquals(9, dsm.usage());
        assertEquals(6, dsm.durableLength());
        dsm.commit();
        assertEquals(8, dsm.usage());
        assertEquals(5, dsm.durableLength());
        ByteBuffer bb1 = ByteBuffer.allocate(dsm.durableLength());
        dsm.write(bb1);
        assertEquals(5, bb1.position());
        bb1.flip();
        dsm = new DiskSpaceManager(bb1);
        assertEquals(8, dsm.usage());
    }
}
