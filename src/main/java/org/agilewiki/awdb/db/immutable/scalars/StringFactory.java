package org.agilewiki.awdb.db.immutable.scalars;

import org.agilewiki.awdb.db.immutable.BaseFactory;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

/**
 * Defines how a String is serialized / deserialized.
 */
public class StringFactory extends BaseFactory {

    /**
     * Create and register this factory.
     *
     * @param factoryRegistry The registry where this factory is registered.
     * @param id              The char used to identify this factory.
     */
    public StringFactory(FactoryRegistry factoryRegistry, char id) {
        super(factoryRegistry, id);
    }

    @Override
    public Class getImmutableClass() {
        return String.class;
    }

    @Override
    public int getDurableLength(Object durable) {
        if (durable == null)
            return 2;
        return 6 + 2 * ((String) durable).length();
    }

    @Override
    public void serialize(Object durable, ByteBuffer byteBuffer) {
        String string = (String) durable;
        byteBuffer.putInt(string.length());
        CharBuffer charBuffer = byteBuffer.asCharBuffer();
        charBuffer.put(string);
        byteBuffer.position(byteBuffer.position() + 2 * charBuffer.position());
    }

    @Override
    public String deserialize(ByteBuffer byteBuffer) {
        int length = byteBuffer.getInt();
        char[] c = new char[length];
        CharBuffer charBuffer = byteBuffer.asCharBuffer();
        charBuffer.get(c);
        byteBuffer.position(byteBuffer.position() + 2 * charBuffer.position());
        return new String(c);
    }
}
