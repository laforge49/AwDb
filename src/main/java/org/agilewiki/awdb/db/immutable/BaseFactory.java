package org.agilewiki.awdb.db.immutable;

/**
 * Base implementation for ImmutableFactory.
 */
public abstract class BaseFactory implements ImmutableFactory {

    /**
     * The registry where this factory is registered.
     */
    public final FactoryRegistry factoryRegistry;

    /**
     * The char identifying the factory.
     */
    public final char id;

    /**
     * Create and register the factory.
     *
     * @param factoryRegistry The registry where the factory is registered.
     * @param id              The char used to identify the factory.
     */
    public BaseFactory(FactoryRegistry factoryRegistry, char id) {
        this.factoryRegistry = factoryRegistry;
        this.id = id;
        factoryRegistry.register(this);
    }

    @Override
    public char getId() {
        return id;
    }
}
