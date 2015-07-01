package org.agilewiki.awdb.db.immutable.collections;

import junit.framework.TestCase;
import org.agilewiki.jactor2.core.impl.Plant;
import org.agilewiki.awdb.db.immutable.BaseRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

public class VersionedListDurableTest extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("cow.db");
            int maxRootBlockSize = 10;
            Db db = new Db(new BaseRegistry(), dbPath, maxRootBlockSize);
            DbFactoryRegistry registry = db.dbFactoryRegistry;

            VersionedListNode l1 = registry.versionedNilList;
            ImmutableFactory factory1 = registry.getImmutableFactory(l1);
            assertTrue(factory1 instanceof VersionedNilListNodeFactory);
            assertEquals(2, factory1.getDurableLength(l1));
            ByteBuffer byteBuffer1 = ByteBuffer.allocate(factory1.getDurableLength(l1));
            factory1.writeDurable(l1, byteBuffer1);
            assertEquals(2, byteBuffer1.position());
            byteBuffer1.flip();
            ImmutableFactory factory2 = registry.readId(byteBuffer1);
            assertTrue(factory2 instanceof VersionedNilListNodeFactory);
            Object object2 = factory2.deserialize(byteBuffer1);
            assertEquals(2, byteBuffer1.position());
            assertTrue(object2.equals(registry.versionedNilList));

            VersionedListNode l2 = l1;
            l2 = l2.add("1");
            l2 = l2.add("2");
            l2 = l2.add("3");
            ImmutableFactory factory3 = registry.getImmutableFactory(l2);
            assertTrue(factory3 instanceof VersionedListNodeFactory);
            assertEquals(122, factory3.getDurableLength(l2));
            ByteBuffer byteBuffer2 = ByteBuffer.allocate(factory3.getDurableLength(l2));
            factory3.writeDurable(l2, byteBuffer2);
            assertEquals(122, byteBuffer2.position());
            byteBuffer2.flip();
            ImmutableFactory factory4 = registry.readId(byteBuffer2);
            assertTrue(factory4 instanceof VersionedListNodeFactory);
            Object object4 = factory4.deserialize(byteBuffer2);
            assertEquals(122, byteBuffer2.position());
            assertEquals("123", String.join("", ((VersionedListNode) object4).flatList(db.getTimestamp())));
        } finally {
            Plant.close();
        }
    }
}
