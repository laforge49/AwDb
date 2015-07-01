package org.agilewiki.awdb.db.immutable.scalars;

import junit.framework.TestCase;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.immutable.Registry;

import java.nio.ByteBuffer;

public class LongTest extends TestCase {
    public void test() throws Exception {
        FactoryRegistry registry = new Registry();
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        Long long1 = 42L;
        ImmutableFactory factory1 = registry.getImmutableFactory(long1);
        assertTrue(factory1 instanceof LongFactory);
        assertEquals(10, factory1.getDurableLength(long1));
        factory1.writeDurable(long1, byteBuffer);
        assertEquals(10, byteBuffer.position());
        byteBuffer.flip();
        ImmutableFactory factory2 = registry.readId(byteBuffer);
        assertTrue(factory2 instanceof LongFactory);
        Object object2 = factory2.deserialize(byteBuffer);
        assertEquals(10, byteBuffer.position());
        assertTrue(object2 instanceof Long);
        long long2 = (Long) object2;
        assertEquals(42l, long2);
    }
}
