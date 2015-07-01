package org.agilewiki.awdb.db.immutable.scalars;

import junit.framework.TestCase;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.immutable.Registry;

import java.nio.ByteBuffer;

public class FloatTest extends TestCase {
    public void test() throws Exception {
        org.agilewiki.awdb.db.immutable.FactoryRegistry registry = new Registry();
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        Float float1 = 42.0f;
        ImmutableFactory factory1 = registry.getImmutableFactory(float1);
        assertTrue(factory1 instanceof FloatFactory);
        assertEquals(6, factory1.getDurableLength(float1));
        factory1.writeDurable(float1, byteBuffer);
        assertEquals(6, byteBuffer.position());
        byteBuffer.flip();
        ImmutableFactory factory2 = registry.readId(byteBuffer);
        assertTrue(factory2 instanceof FloatFactory);
        Object object2 = factory2.deserialize(byteBuffer);
        assertEquals(6, byteBuffer.position());
        assertTrue(object2 instanceof Float);
        float float2 = (Float) object2;
        assertEquals(42.0f, float2);
    }
}
