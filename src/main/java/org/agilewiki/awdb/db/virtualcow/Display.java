package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.immutable.collections.ListAccessor;
import org.agilewiki.awdb.db.immutable.collections.MapAccessor;
import org.agilewiki.awdb.db.immutable.collections.VersionedMapNode;

/**
 * Diagnostic display of database contents.
 */
public class Display {
    public static void all(Db db, long timestamp) {
        System.out.println("\n\n--Database Dump--");
        MapAccessor mapAccessor = db.mapAccessor();
        for (ListAccessor la: mapAccessor) {
            VersionedMapNode vmn = (VersionedMapNode) la.get(0);
            if (!vmn.isEmpty(timestamp))
                vmn(vmn.mapAccessor(timestamp), (String) la.key());
        }
    }

    public static void vmn(MapAccessor ma, String id) {
        System.out.println("\nvmn id: " + id);
        for (ListAccessor la: ma) {
            vln(la);
        }
    }

    public static void vln(ListAccessor la) {
        System.out.println("    " + la.key() + " = " + la.flatList());
    }
}
