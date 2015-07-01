package org.agilewiki.awdb.db.immutable;

import org.agilewiki.awdb.db.immutable.scalars.NullFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements a cascading factory registry.
 */
public class CascadingRegistry implements FactoryRegistry {
    public final CascadingRegistry parent;

    protected final ConcurrentHashMap<Character, ImmutableFactory> idMap =
            new ConcurrentHashMap<>(16, 0.75f, 1);
    protected final ConcurrentHashMap<Class, ImmutableFactory> classMap =
            new ConcurrentHashMap<>(16, 0.75f, 1);

    /**
     * Creates the registry and registers the default factories.
     */
    public CascadingRegistry() {
        parent = null;
    }

    /**
     * Create a registry with a parent registry.
     *
     * @param parent The parent registry.
     */
    public CascadingRegistry(CascadingRegistry parent) {
        this.parent = parent;
    }

    @Override
    public void register(ImmutableFactory factory) {
        idMap.put(factory.getId(), factory);
        classMap.put(factory.getImmutableClass(), factory);
    }

    @Override
    public ImmutableFactory getImmutableFactory(char id) {
        ImmutableFactory f = idMap.get(id);
        if (f != null || parent == null)
            return f;
        return parent.getImmutableFactory(id);
    }

    @Override
    public ImmutableFactory getImmutableFactory(Class c) {
        ImmutableFactory factory = classMap.get(c);
        if (factory != null)
            return factory;
        if (parent != null)
            return parent.getImmutableFactory(c);
        throw new IllegalArgumentException("Unknown class: " + c.getName());
    }

    @Override
    public ImmutableFactory getImmutableFactory(Object immutable) {
        Class c = immutable == null ? NullFactory.class : immutable.getClass();
        ImmutableFactory factory = classMap.get(c);
        if (factory != null)
            return factory.getImmutableFactory(immutable);
        if (parent != null)
            return parent.getImmutableFactory(immutable);
        throw new IllegalArgumentException("Unknown class: " + c.getName());
    }

    @Override
    public ImmutableFactory readId(ByteBuffer byteBuffer) {
        char id = byteBuffer.getChar();
        ImmutableFactory factory = getImmutableFactory(id);
        if (factory == null)
            throw new IllegalStateException("Unknown durable id: " + id);
        return factory;
    }
}
