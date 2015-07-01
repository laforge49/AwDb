package org.agilewiki.awdb.db.ids.composites;

import org.agilewiki.awdb.db.ids.NameId;
import org.agilewiki.awdb.db.ids.ValueId;
import org.agilewiki.awdb.db.immutable.collections.*;
import org.agilewiki.awdb.db.virtualcow.Db;

/**
 * An implementation of secondary ids for a Versioned Map Node (VMN).
 */
public class SecondaryId {
    /**
     * Identifies an id as a composite for an inverted secondary id.
     */
    public static final String SECONDARY_INV = "$C";

    /**
     * Identifies an id as a composite for a secondary id.
     */
    public static final String SECONDARY_ID = "$D";

    /**
     * Returns a composite key for the inverse of a secondary id.
     *
     * @param vmnId  The id of the VMN.
     * @param typeId The type of secondary key.
     * @return The composite key.
     */
    public static String secondaryInv(String vmnId, String typeId) {
        NameId.validateAnId(vmnId);
        NameId.validateId(typeId);
        return SECONDARY_INV + vmnId + typeId;
    }

    /**
     * Returns a composite id for a secondary identifier.
     *
     * @param typeId  The type of secondary key.
     * @param valueId The value of the secondary key.
     * @return The composite id.
     */
    public static String secondaryId(String typeId, String valueId) {
        NameId.validateId(typeId);
        ValueId.validateAnId(valueId);
        return SECONDARY_ID + typeId + valueId;
    }

    /**
     * Validate a secondary id.
     *
     * @param secondaryId The secondary id.
     */
    public static void validateSecondaryId(String secondaryId) {
        if (!secondaryId.startsWith(SECONDARY_ID + "$"))
            throw new IllegalArgumentException("not a secondary id: " + secondaryId);
        int i = secondaryId.indexOf('$', 4);
        if (i < 0)
            throw new IllegalArgumentException("not a secondary id: " + secondaryId);
        if (!secondaryId.substring(i).startsWith("$"))
            throw new IllegalArgumentException("not a secondary id: " + secondaryId);
    }

    /**
     * Returns the name id of the secondary inv type.
     *
     * @param secondaryInv A secondary inv.
     * @return The name id.
     */
    public static String secondaryInvType(String secondaryInv) {
        if (!secondaryInv.startsWith(SECONDARY_INV))
            throw new IllegalArgumentException("not a secondary inv: " + secondaryInv);
        int i = secondaryInv.indexOf('$', 3);
        if (i < 0)
            throw new IllegalArgumentException("not a secondary inv: " + secondaryInv);
        String nameId = secondaryInv.substring(i);
        NameId.validateId(nameId);
        return nameId;
    }

    public static String secondaryInvVmn(String secondaryInv) {
        if (!secondaryInv.startsWith(SECONDARY_INV))
            throw new IllegalArgumentException("not a secondary inv: " + secondaryInv);
        int i = secondaryInv.indexOf('$', 3);
        if (i < 0)
            throw new IllegalArgumentException("not a secondary inv: " + secondaryInv);
        String nameId = secondaryInv.substring(2, i);
        NameId.validateAnId(nameId);
        return nameId;
    }

    /**
     * Returns the name id of the secondary id type.
     *
     * @param secondaryId A secondary id.
     * @return The name id.
     */
    public static String secondaryIdType(String secondaryId) {
        if (!secondaryId.startsWith(SECONDARY_ID))
            throw new IllegalArgumentException("not a secondary id: " + secondaryId);
        int i = secondaryId.indexOf('$', 3);
        if (i < 0)
            throw new IllegalArgumentException("not a secondary id: " + secondaryId);
        String nameId = secondaryId.substring(2, i);
        NameId.validateId(nameId);
        return nameId;
    }

    /**
     * Returns the value id of the secondary id type.
     *
     * @param secondaryId A secondary id.
     * @return The value id.
     */
    public static String secondaryIdValue(String secondaryId) {
        if (!secondaryId.startsWith(SECONDARY_ID))
            throw new IllegalArgumentException("not a secondary id: " + secondaryId);
        int i = secondaryId.indexOf('$', 3);
        if (i < 0)
            throw new IllegalArgumentException("not a secondary id: " + secondaryId);
        String valueId = secondaryId.substring(i);
        ValueId.validateAnId(valueId);
        return valueId;
    }

    /**
     * Iterates over the secondary types.
     *
     * @param db    The database.
     * @param vmnId The id of a VMN.
     * @return An iterable over the types.
     */
    public static PeekABoo<String> typeIdIterable(Db db, String vmnId) {
        MapAccessor ma = db.mapAccessor();
        PeekABoo<ListAccessor> lait = ma.iterator(SECONDARY_INV + vmnId);
        return new PeekABooMap<ListAccessor, String>(lait) {
            @Override
            protected String transform(ListAccessor value) {
                String secondaryInv = value.key().toString();
                int i = secondaryInv.lastIndexOf("$");
                return secondaryInv.substring(i);
            }

            @Override
            protected String transformString(String secondaryInv) {
                int i = secondaryInv.lastIndexOf("$");
                return secondaryInv.substring(i);
            }

            @Override
            protected String reverseTransformString(String typeId) {
                return secondaryInv(vmnId, typeId);
            }
        };
    }

    /**
     * Iterates over the secondary keys.
     *
     * @param db        The database.
     * @param vmnId     The id of the VMN.
     * @param typeId    The type of secondary key.
     * @param timestamp The time of the query.
     * @return The Iterable, or null.
     */
    public static PeekABoo<String> secondaryIdIterable(Db db, String vmnId, String typeId, long timestamp) {
        PeekABoo<String> vit = db.keysIterable(secondaryInv(vmnId, typeId), timestamp);
        return new PeekABooMap<String, String>(vit) {
            @Override
            protected String transform(String value) {
                return secondaryId(typeId, value);
            }

            @Override
            protected String transformString(String value) {
                return secondaryId(typeId, value);
            }

            @Override
            protected String reverseTransformString(String secondaryId) {
                return secondaryIdValue(secondaryId);
            }
        };
    }

    /**
     * Iterates over the ids of the VMNs referenced by a secondary id.
     *
     * @param db          The database.
     * @param secondaryId The secondary id.
     * @param timestamp   The time of the query.
     * @return The Iterable, or null.
     */
    public static PeekABoo<String> vmnIdIterable(Db db, String secondaryId, long timestamp) {
        return db.keysIterable(secondaryId, timestamp);
    }

    /**
     * Returns true iff the vmn has the given secondary id.
     *
     * @param db          The database.
     * @param vmlId       The id of the vml.
     * @param secondaryId The secondary id.
     * @param timestamp   The time of the query.
     * @return True if the secondary key is present.
     */
    public static boolean hasSecondaryId(Db db, String vmlId, String secondaryId, long timestamp) {
        NameId.validateAnId(vmlId);
        VersionedListNode vln = db.versionedListNode(secondaryId, vmlId);
        if (vln == null)
            return false;
        return !vln.isEmpty(timestamp);
    }

    /**
     * Returns true iff the vmn has the given secondary id.
     *
     * @param db          The database.
     * @param vmlId       The id of the vml.
     * @param typeId      The id of the key.
     * @param valueId     The value of the key.
     * @param timestamp   The time of the query.
     * @return True if the secondary key is present.
     */
    public static boolean hasSecondaryId(Db db, String vmlId, String typeId, String valueId, long timestamp) {
        return hasSecondaryId(db, vmlId, secondaryId(typeId, valueId), timestamp);
    }

    /**
     * Add a secondary key to a vml if not already present.
     *
     * @param db          The database.
     * @param vmnId       The id of the vmn.
     * @param secondaryId The secondary id.
     */
    public static void createSecondaryId(Db db, String vmnId, String secondaryId) {
        if (hasSecondaryId(db, vmnId, secondaryId, db.getTimestamp()))
            return;
        db.set(secondaryId, vmnId, true);
        String valueId = secondaryIdValue(secondaryId);
        String typeId = secondaryIdType(secondaryId);
        db.set(secondaryInv(vmnId, typeId),
                valueId,
                true);
        if (vmnId != db.getJEName())
            db.updateJournal(vmnId);
        db.updateJournal(typeId);
    }

    /**
     * Remove a secondary key from a vml if present.
     *
     * @param db          The database.
     * @param vmnId       The id of the vmn.
     * @param secondaryId The secondary id.
     */
    public static void removeSecondaryId(Db db, String vmnId, String secondaryId) {
        if (!hasSecondaryId(db, vmnId, secondaryId, db.getTimestamp()))
            return;
        db.clearList(secondaryId, vmnId);
        String valueId = secondaryIdValue(secondaryId);
        String typeId = secondaryIdType(secondaryId);
        db.clearList(secondaryInv(vmnId, typeId),
                valueId);
        if (vmnId != db.getJEName())
            db.updateJournal(vmnId);
        db.updateJournal(typeId);
    }
}
