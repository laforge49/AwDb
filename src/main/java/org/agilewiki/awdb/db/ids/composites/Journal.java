package org.agilewiki.awdb.db.ids.composites;

import org.agilewiki.awdb.db.ids.Timestamp;
import org.agilewiki.awdb.db.ids.ValueId;
import org.agilewiki.awdb.db.immutable.collections.PeekABoo;
import org.agilewiki.awdb.db.virtualcow.Db;

/**
 * Connecting journal entries to the things they update.
 */
public class Journal {
    /**
     * Prefix to a timestamp/id composite.
     */
    public static final String MODIFIES_ID = "$A";

    /**
     * The key to the list of timestamps identifying the
     * journal entry which modified the vmn.
     */
    public static final String JOURNAL_ID = "$B";

    /**
     * Returns a composite id used to connect a journal entry to the VMN it modifies.
     *
     * @param timestampId    The id of the journal entry.
     * @return A composite of 2 ids.
     */
    public static String modifiesId(String timestampId) {
        Timestamp.validateId(timestampId);
        return MODIFIES_ID + timestampId;
    }

    /**
     * Returns a composite id used to connect a VMN to the journal entry which modified it.
     * @param id    The id of the VMN.
     * @return A composite of 2 ids.
     */
    public static String journalId(String id) {
        ValueId.validateAnId(id);
        return JOURNAL_ID + id;
    }

    /**
     * Iterates over the ids of the Versioned Map Nodes (VMNs) modified by the
     * given journal entry.
     *
     * @param db             The database.
     * @param timestampId    The timestampId of the journal entry.
     * @param longTimestamp The time of the query.
     * @return The iterable.
     */
    public static PeekABoo<String> modifies(Db db, String timestampId, long longTimestamp) {
        return db.keysIterable(modifiesId(timestampId), longTimestamp);
    }

    /**
     * Iterates over the timestamps of the journal entries which modified a given
     * Virtual Map Node (VMN).
     *
     * @param db    The database.
     * @param id    The id of the VMN.
     * @param longTimestamp The time of the query.
     * @return The iterable.
     */
    public static PeekABoo<String> journal(Db db, String id, long longTimestamp) {
        return db.keysIterable(journalId(id), longTimestamp);
    }
}
