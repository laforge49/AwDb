package org.agilewiki.awdb.db.immutable;

/**
 * Initialize the factory registry.
 */
public class Registry extends CascadingRegistry {

    /**
     * Creates the registry and registers the default factories.
     */
    public Registry() {
        this(new BaseRegistry());
    }

    /**
     * Create a cascading factory registry.
     *
     * @param parent The parent registry.
     */
    public Registry(CascadingRegistry parent) {
        super(parent);
    }
}
