package org.agilewiki.awdb.db.immutable.scalars;

import junit.framework.TestCase;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.immutable.Registry;

import java.nio.ByteBuffer;

public class IntegerTest extends TestCase {
    public void test() throws Exception {
        FactoryRegistry registry = new Registry();
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        Integer integer1 = 42;
        ImmutableFactory factory1 = registry.getImmutableFactory(integer1);
        assertTrue(factory1 instanceof IntegerFactory);
        assertEquals(6, factory1.getDurableLength(integer1));
        factory1.writeDurable(integer1, byteBuffer);
        assertEquals(6, byteBuffer.position());
        byteBuffer.flip();
        ImmutableFactory factory2 = registry.readId(byteBuffer);
        assertTrue(factory2 instanceof IntegerFactory);
        Object object2 = factory2.deserialize(byteBuffer);
        assertEquals(6, byteBuffer.position());
        assertTrue(object2 instanceof Integer);
        int int2 = (Integer) object2;
        assertEquals(42, int2);
    }
}
