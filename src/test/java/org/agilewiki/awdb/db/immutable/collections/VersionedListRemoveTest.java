package org.agilewiki.awdb.db.immutable.collections;

import junit.framework.TestCase;
import org.agilewiki.jactor2.core.impl.Plant;
import org.agilewiki.awdb.db.immutable.BaseRegistry;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class VersionedListRemoveTest extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("cow.db");
            int maxRootBlockSize = 10;
            Db db = new Db(new BaseRegistry(), dbPath, maxRootBlockSize);
            DbFactoryRegistry registry = db.dbFactoryRegistry;

            VersionedListNode l1 = registry.versionedNilList;

            db._setTimestamp(1);
            l1 = l1.remove(-1);
            l1 = l1.remove(0);
            l1 = l1.remove(1);

            db._setTimestamp(2);
            VersionedListNode l2 = registry.versionedNilList.add("a");
            db._setTimestamp(3);
            l2 = l2.add("b");
            db._setTimestamp(4);
            l2 = l2.add("c");
            db._setTimestamp(5);
            l2 = l2.add("d");
            db._setTimestamp(6);
            l2 = l2.add("e");
            db._setTimestamp(7);
            l2 = l2.add("f");
            db._setTimestamp(8);
            l2 = l2.add("g");

            db._setTimestamp(9);
            l2 = l2.remove(-3);
            db._setTimestamp(10);
            l2 = l2.remove(-2);
            db._setTimestamp(11);
            l2 = l2.remove(-1);
            assertEquals(7, l2.totalSize());
            db._setTimestamp(12);
            l2 = l2.remove(0);
            db._setTimestamp(13);
            l2 = l2.remove(1);
            db._setTimestamp(14);
            l2 = l2.remove(2);
            db._setTimestamp(15);
            l2 = l2.remove(3);
            db._setTimestamp(16);
            l2 = l2.remove(4);
            db._setTimestamp(17);
            l2 = l2.remove(5);
            db._setTimestamp(18);
            l2 = l2.remove(6);
            db._setTimestamp(19);
            l2 = l2.remove(7);
            db._setTimestamp(20);
            l2 = l2.remove(8);
            assertEquals("abcdefg", String.join("", l2.flatList(8)));
            assertEquals("bcdefg", String.join("", l2.flatList(12)));
            assertEquals("cdefg", String.join("", l2.flatList(13)));
            assertEquals("defg", String.join("", l2.flatList(14)));
            assertEquals("g", String.join("", l2.flatList(17)));
            assertEquals("", String.join("", l2.flatList(21)));

            assertEquals(6, l2.firstIndex(17));
            assertEquals(-1, l2.lastIndex(22));

            VersionedListNode copy = l2.copyList(16);
            assertEquals("e", String.join("", copy.flatList(6)));
            assertEquals("efg", String.join("", copy.flatList(15)));
            assertEquals(3, l2.size(15));
            assertEquals("g", String.join("", copy.flatList(17)));
            Iterator it = copy.iterator(15);
            assertTrue(it.hasNext());
            assertEquals("e", it.next());
            assertTrue(it.hasNext());
            assertEquals("f", it.next());
            assertTrue(it.hasNext());
            assertEquals("g", it.next());
            assertFalse(it.hasNext());

            db._setTimestamp(30);
            copy = copy.clearList();
            assertEquals(0, copy.size(30));
        } finally {
            Plant.close();
        }
    }
}
