package org.agilewiki.awdb.db.immutable.scalars;

import junit.framework.TestCase;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.immutable.Registry;

import java.nio.ByteBuffer;

public class StringTest extends TestCase {
    public void test() throws Exception {
        FactoryRegistry registry = new Registry();
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        String string1 = "abc";
        ImmutableFactory factory1 = registry.getImmutableFactory(string1);
        assertTrue(factory1 instanceof StringFactory);
        assertEquals(12, factory1.getDurableLength(string1));
        factory1.writeDurable(string1, byteBuffer);
        assertEquals(12, byteBuffer.position());
        byteBuffer.flip();
        ImmutableFactory factory2 = registry.readId(byteBuffer);
        assertTrue(factory2 instanceof StringFactory);
        Object object2 = factory2.deserialize(byteBuffer);
        assertEquals(12, byteBuffer.position());
        assertTrue(object2 instanceof String);
        String string2 = (String) object2;
        assertEquals("abc", string2);
    }
}
