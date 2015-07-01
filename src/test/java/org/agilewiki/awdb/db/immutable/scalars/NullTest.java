package org.agilewiki.awdb.db.immutable.scalars;

import junit.framework.TestCase;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.immutable.Registry;

import java.nio.ByteBuffer;

public class NullTest extends TestCase {
    public void test() throws Exception {
        FactoryRegistry registry = new Registry();
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        Integer integer1 = null;
        ImmutableFactory factory1 = registry.getImmutableFactory(integer1);
        assertTrue(factory1 instanceof NullFactory);
        assertEquals(2, factory1.getDurableLength(integer1));
        factory1.writeDurable(integer1, byteBuffer);
        assertEquals(2, byteBuffer.position());
        byteBuffer.flip();
        ImmutableFactory factory2 = registry.readId(byteBuffer);
        assertTrue(factory2 instanceof NullFactory);
        Object object2 = factory2.deserialize(byteBuffer);
        assertEquals(2, byteBuffer.position());
        assertNull(object2);
    }
}
