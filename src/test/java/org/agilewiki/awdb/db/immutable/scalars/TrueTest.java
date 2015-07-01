package org.agilewiki.awdb.db.immutable.scalars;

import junit.framework.TestCase;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.immutable.Registry;

import java.nio.ByteBuffer;

public class TrueTest extends TestCase {
    public void test() throws Exception {
        FactoryRegistry registry = new Registry();
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        Boolean boolean1 = true;
        ImmutableFactory factory1 = registry.getImmutableFactory(boolean1);
        assertTrue(factory1 instanceof TrueFactory);
        assertEquals(2, factory1.getDurableLength(boolean1));
        factory1.writeDurable(boolean1, byteBuffer);
        assertEquals(2, byteBuffer.position());
        byteBuffer.flip();
        ImmutableFactory factory2 = registry.readId(byteBuffer);
        assertTrue(factory2 instanceof TrueFactory);
        Object object2 = factory2.deserialize(byteBuffer);
        assertEquals(2, byteBuffer.position());
        assertTrue(object2.equals(true));
    }
}
