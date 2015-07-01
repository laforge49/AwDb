package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.virtualcow.Db;

/**
 * Iterates over the non-empty ids.
 */
public class IdPeekABooable implements PeekABooable<String> {
    final PeekABoo<String> peekABoo;

    public IdPeekABooable(Db db, String prefix, long timestamp) {
        MapAccessor ma = db.mapAccessor();
        PeekABoo<ListAccessor> iterator = ma.iterator(prefix);
        PeekABooFilter<ListAccessor> filter = new PeekABooFilter<ListAccessor>(iterator) {
            @Override
            protected boolean select(ListAccessor value) {
                if (value == null)
                    return false;
                VersionedMapNode vmn = (VersionedMapNode) value.get(0);
                if (vmn == null)
                    return false;
                return !vmn.isEmpty(timestamp);
            }
        };
        peekABoo = new PeekABooMap<ListAccessor, String>(filter) {
            @Override
            protected String transform(ListAccessor value) {
                return ((String) value.key()).substring(prefix.length());
            }

            @Override
            protected String transformString(String value) {
                return value.substring(prefix.length());
            }

            @Override
            protected String reverseTransformString(String value) {
                return prefix + value;
            }
        };
    }

    @Override
    public PeekABoo<String> iterator() {
        return peekABoo;
    }
}
