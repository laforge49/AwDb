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

public class VersionedMapDurableTest extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("cow.db");
            int maxRootBlockSize = 10;
            Db db = new Db(new BaseRegistry(), dbPath, maxRootBlockSize);
            DbFactoryRegistry registry = db.dbFactoryRegistry;

            VersionedMapNode m1 = registry.versionedNilMap;
            ImmutableFactory factory1 = registry.getImmutableFactory(m1);
            assertTrue(factory1 instanceof VersionedNilMapNodeFactory);
            assertEquals(2, factory1.getDurableLength(m1));
            ByteBuffer byteBuffer1 = ByteBuffer.allocate(factory1.getDurableLength(m1));
            factory1.writeDurable(m1, byteBuffer1);
            assertEquals(2, byteBuffer1.position());
            byteBuffer1.flip();
            ImmutableFactory factory2 = registry.readId(byteBuffer1);
            assertTrue(factory2 instanceof VersionedNilMapNodeFactory);
            Object object2 = factory2.deserialize(byteBuffer1);
            assertEquals(2, byteBuffer1.position());
            assertTrue(object2.equals(registry.versionedNilMap));

            VersionedMapNode m2 = m1;
            m2 = m2.add("a", "1");
            m2 = m2.add("a", "2");
            m2 = m2.add("a", "3");
            ImmutableFactory factory3 = registry.getImmutableFactory(m2);
            assertTrue(factory3 instanceof VersionedMapNodeFactory);
            assertEquals(144, factory3.getDurableLength(m2));
            ByteBuffer byteBuffer2 = ByteBuffer.allocate(factory3.getDurableLength(m2));
            factory3.writeDurable(m2, byteBuffer2);
            assertEquals(144, byteBuffer2.position());
            byteBuffer2.flip();
            ImmutableFactory factory4 = registry.readId(byteBuffer2);
            assertTrue(factory4 instanceof VersionedMapNodeFactory);
            Object object4 = factory4.deserialize(byteBuffer2);
            assertEquals(144, byteBuffer2.position());
            assertEquals("123", String.join("", ((VersionedMapNode) object4).getList("a").flatList(db.getTimestamp())));
        } finally {
            Plant.close();
        }
    }
}
