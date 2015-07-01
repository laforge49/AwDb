package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.immutable.collections.MapNode;

/**
 * A transaction simply transforms a map list.
 */
public interface Transaction {
    /**
     * Transforms a map list.
     *
     * @param db        The database to be updated.
     * @param tMapNode  The durable content of the transaction.
     */
    void transform(Db db, MapNode tMapNode);

    /**
     * Transaction timeout in milliseconds.
     * Set to Integer.MAX_VALUE by default.
     *
     * @return The max time in milliseconds the transaction can take.
     */
    default int timeoutMillis() {
        return Integer.MAX_VALUE;
    }
}
