package org.agilewiki.awdb.db.immutable.scalars;

import junit.framework.TestCase;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.immutable.Registry;

import java.nio.ByteBuffer;

public class DoubleTest extends TestCase {
    public void test() throws Exception {
        FactoryRegistry registry = new Registry();
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        Double double1 = 42.0D;
        ImmutableFactory factory1 = registry.getImmutableFactory(double1);
        assertTrue(factory1 instanceof DoubleFactory);
        assertEquals(10, factory1.getDurableLength(double1));
        factory1.writeDurable(double1, byteBuffer);
        assertEquals(10, byteBuffer.position());
        byteBuffer.flip();
        ImmutableFactory factory2 = registry.readId(byteBuffer);
        assertTrue(factory2 instanceof DoubleFactory);
        Object object2 = factory2.deserialize(byteBuffer);
        assertEquals(10, byteBuffer.position());
        assertTrue(object2 instanceof Double);
        double double2 = (Double) object2;
        assertEquals(42.0D, double2);
    }
}
