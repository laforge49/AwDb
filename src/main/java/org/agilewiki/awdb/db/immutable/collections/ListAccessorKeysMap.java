package org.agilewiki.awdb.db.immutable.collections;

/**
 * Iterates over the vmn keys.
 */
public class ListAccessorKeysMap extends PeekABooMap<ListAccessor, String> {

    public ListAccessorKeysMap(PeekABoo<ListAccessor> peekABoo) {
        super(peekABoo);
    }

    @Override
    protected String transform(ListAccessor value) {
        return (String) value.key();
    }

    @Override
    protected String transformString(String value) {
        return value;
    }

    @Override
    protected String reverseTransformString(String value) {
        return value;
    }
}
